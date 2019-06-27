import gym
from gym import spaces
import numpy as np
import random
from gym.envs.postgres_idx_advisor.envs.QueryExecutor import QueryExecutor


class PostgresIdxAdvisorEnv(gym.Env):

    metadata = {'render.modes': ['human']}


    def __init__(self):
        """
            Declares the variables of the environment.
            Important variables: action_space, observation_space
            Discrete : Action set with 60 actions {0, 1, 2,.........., 59}
            Box: (low = lowest value in the matrix, high = highest value in the matrix, shape of the state matrix)
        """
        #self.env = self
        self.k = 0
        self.reward = 0
        self.game_over = False
        self.counter = 0
        self.k_idx = 0
        self.cost_initial = 0
        self.cost_idx_advisor = 0
        self.observation = None
        self.cost_prev = None
        self.action_space = spaces.Discrete(60)
        self.value = 0
        self.value_prev = 999
        #self.observation_space = spaces.Box(low=0, high=1, shape=(8, 61), dtype=np.float32)
        self.observation_space = spaces.Box(low=0, high=1, shape=(8, 61, 1), dtype=np.float32)
        self.queries_list = None
        self.all_predicates = None
        self.idx_advisor_suggested_indexes = None
        self.evaluation_mode = None
        self.agent = None


    def set_eval_mode(self, eval_mode):
        """
        :param eval_mode: if evaluation phase then True else False
            Sets the evaluation mode returned by the agent"""
        self.evaluation_mode = eval_mode

    def reset(self):
        """
            :return: initial observation
                Reset is supposed to be used after every game end criteria
                Its purpose is to reinitialize the necessary variables
                Important variables: observation, initial cost, best cost, k (No of indexes that the agent is allowed to set)
                k_offset is the offset for the different number of indexes that agent and env needs to be tested on
        """
        self.reward = 0.0
        self.game_over = False
        self.counter = 0
        self.k_idx = 0

        k_offset, train_file, test_file, self.agent = QueryExecutor.get_gin_properties()

        # Enters Evaluation mode
        if self.evaluation_mode:
            self.observation, self.cost_initial, self.cost_idx_advisor, k_value = self.init_observation(test_file, 0)
            self.k = k_value

        # Enters training mode
        elif not self.evaluation_mode:
            self.observation, self.cost_initial, self.cost_idx_advisor, k_value = self.init_observation(train_file, k_offset)
            self.k = k_value + k_offset

        self.cost_prev = self.cost_initial
        self.value = 0.0
        self.value_prev = 1/self.cost_initial

        if self.agent.lower() != 'dopamine':
            #print(self.agent, 'flattening')
            self.observation = self.observation.flatten().reshape(8, 61, 1)

        return self.observation

    def init_observation(self, filename, k_offset):
        """
        :param filename: Contains the filename which needs to be considered. (train.sql and test.sql)
        :param k_offset: It is the offset that can be used to control the number of indexes
        :return: observation, initial cost, index advisor cost, original number of index advisor
            Initializes the state, initial cost and the best cost on every reset
            init_variables(): Sends the queries to the DB and get selectivity matrix
            get_best_cost(): gets the best cost with the number of indexes returned by the index advisor
            get_best_index_combination() : returns the best cost with a limit on the number of indexes
        """
        self.queries_list, self.all_predicates, self.idx_advisor_suggested_indexes = QueryExecutor.init_variables(filename)
        self.observation = QueryExecutor.create_observation_space(self.queries_list)
        self.cost_initial = QueryExecutor.get_initial_cost(self.queries_list)
        if k_offset >= 0:
            self.cost_idx_advisor = QueryExecutor.get_best_cost(self.queries_list, self.idx_advisor_suggested_indexes)
        elif k_offset < 0:
            self.cost_idx_advisor = QueryExecutor.get_best_index_combination(self.queries_list, self.idx_advisor_suggested_indexes, len(self.idx_advisor_suggested_indexes)+k_offset)

        return self.observation, self.cost_initial, self.cost_idx_advisor, len(self.idx_advisor_suggested_indexes)

    def _take_action(self, action):
        """
        :param action:  action returned by the agent
        :return: returns the new observation
            Transfers the action to the DB and gets a new state
        """
        # Something which takes action and returns te next state with hypothetical indexes set
        if self.observation[0, action] != 1:
            self.observation, cost_action = QueryExecutor.generate_next_state(self.queries_list, action, self.observation)
            switch_correct = 1
        # switch the position at that action to 1
        # if index is not yet set set switch_correct to 1 else 0
        else:
            cost_action = float("inf")
            switch_correct = 0

        return self.observation, cost_action, switch_correct

    def step(self, action):
        """
        :param action: action returned by the agent
        :return: returns the new observation to the agent
            Executes each step in the env
            Each step involves:
                - Translating agent action to DB
                - Translating the DB response to agent response
        """
        self.observation, cost_agent_idx, switch_correct = self._take_action(action)
        if switch_correct == 1 and self.k_idx < self.k:
            self.value = self.calculate_value(cost_agent_idx)
            if self.value_prev < self.value:
                self.game_over = False
                self.reward = 1
                self.value_prev = self.value
                self.cost_prev = cost_agent_idx
            else:
                QueryExecutor.remove_all_hypo_indexes()
                self.game_over = True
                self.reward = self.calculate_reward(self.cost_initial, self.cost_idx_advisor, self.cost_prev, self.counter)
        else:
            QueryExecutor.remove_all_hypo_indexes()
            self.game_over = True
            self.reward = self.calculate_reward(self.cost_initial, self.cost_idx_advisor, self.cost_prev, self.counter)

        self.k_idx += 1
        self.counter += 1

        if self.agent.lower() != 'dopamine':
            #print(self.agent, 'flattening')
            self.observation = self.observation.flatten().reshape(8, 61, 1)

        QueryExecutor.check_step_variables(self.observation, cost_agent_idx, switch_correct, self.k, self.k_idx, self.value, self.value_prev, self.game_over, self.reward, self.counter, action)

        return self.observation, self.reward, self.game_over, {}

    def render(self, mode='human', close=False):
        """
        :param mode: human is human readable
            Print some statements you want to prints
        """
        print(self.counter, "\t", self.reward, "\t", "\t", self.observation[0, :])

    @staticmethod
    def calculate_value(cost):
        """
        :param cost: cost value float32
        :return: value
            Calculates the value of the input cost
        """
        value = 1/cost
        return value

    def calculate_reward(self, cost_initial, cost_idx_advisor, cost_agent_idx, counter):
        """
        :param cost_initial: initial cost
        :param cost_idx_advisor: index advisor cost
        :param cost_agent_idx: cost based on the agents action
        :param counter: how many number of steps it took to reach game end
        :return: reward
            Calculates reward
        """
        denom =(self.calculate_value(cost_idx_advisor) - self.calculate_value(cost_initial))
        if denom==0:
            denom=1
        rew = (((self.calculate_value(cost_agent_idx) - self.calculate_value(cost_initial))/denom)*100) - counter
        return rew
