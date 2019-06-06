import gym
from gym import spaces
import numpy as np
import random
from gym.envs.postgres_idx_advisor.envs.QueryExecutor import QueryExecutor


class PostgresIdxAdvisorEnv(gym.Env):

    metadata = {'render.modes': ['human']}

    def __init__(self):
        # super(PostgresIdxAdvisorEnv, self).__init__()
        self.k = 0
        #self.reward_range(1, 100)
        self.reward = 0
        self.done = 0
        self.counter = 0
        self.k_idx = 0
        self.cost_initial = 0
        self.cost_idx_advisor = 0
        self.observation = None
        # Actions of the format
        # Action set with 60 actions {0, 1, 2,.........., 59}
        self.action_space = spaces.Discrete(60)
        self.value = 0
        self.value_prev = 999
        # Inititalisation of Observation Space using Box
        # (low = lowest value in the matrix, high = highest value in the matrix, shape of the state matrix)
        self.observation_space = spaces.Box(low=0, high=1, shape=(8, 60), dtype=np.float32)
        self.queries_list = None
        self.all_predicates = None
        self.idx_advisor_suggested_indexes = None

    # reset is supposed to be used after every game end criteria
    def reset(self):
        #self.reward_range(1, 100)
        self.reward = 0.0
        self.done = False
        self.counter = 1
        self.k_idx = 0
        self.observation, self.cost_initial, self.cost_idx_advisor = self.init_observation()
        #self.cost_initial = 10000  # Some function to retrieve the cost
        #self.cost_idx_advisor = 7000  # Some function to retrieve the cost
        self.value = 0.0
        self.value_prev = float("inf")
        self.k = 10
        return self.observation

    def init_observation(self):
        # send the queries to the DB and get selectivity matrix
        """obs = np.array([[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              [0.1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              [0,0.2,0,0,0,0,0,0,0,0,0,0,0,0,0,0.8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              [0,0,0.3,0,0,0,0,0,0,0,0,0,0,0,0.7,0,0,0,0,0,0,0,0,0,0.5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              [0,0,0,0.1,0,0,0,0,0,0,0,0,0,0.1,0,0,0,0,0,0,0,0,0,0,0,0.5,0,0,0,0,0,0,0,0,0,0.1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              [0,0,0,0,0.3,0,0,0,0,0,0,0,0.1,0,0,0,0,0,0,0,0,0,0,0,0,0,0.5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              [0,0,0,0,0,0.6,0,0,0,0,0,0.2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.5,0,0,0,0,0,0,0.1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
              [0,0,0,0,0,0,0.9,0,0,0,0.3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]])"""
        self.observation, self.queries_list, self.idx_advisor_suggested_indexes = QueryExecutor.init_variables()
        self.cost_initial = QueryExecutor.get_initial_cost(self.queries_list)
        self.cost_idx_advisor = QueryExecutor.get_best_cost(self.queries_list)

        return self.observation, self.cost_initial, self.cost_idx_advisor

    def _next_observation(self, action):
        # Something which takes action and returns te next state with hypothetical indexes set

        #obs = self.observation
        if self.observation[0, action] != 1:
            #obs[0, action] = 1
            obs, cost_action = QueryExecutor.generate_next_state(self.queries_list, action, self.observation)
            switch_correct = 1
        # switch the position at that action to 1
        # if index is not yet set set switch_correct to 1 else 0
        else:
            switch_correct = 0

        return obs, cost_action, switch_correct

    # If this doesnt work then somehow need to figure out hoe to pass the observation to perform the action
    def step(self, action):
        # Execute one time step within the environment
        obs, cost_agent_idx, switch_correct = self._take_action(action)
        if switch_correct == 1 and self.k_idx < self.k:
            self.value = self.calculate_value(cost_agent_idx)
            if self.value_prev > self.value:
                self.done = 0
                self.reward = 1
                self.k_idx += 1
                self.value_prev = self.value
            else:
                self.done = True
                self.reward = 100 - self.counter
        else:
            self.done = True
            self.reward = 100 - self.counter

        self.counter += self.counter
        self.k_idx += self.k_idx
        return obs, self.reward, self.done, {}

    def _take_action(self,  action):
        # Set hypothetical indexes and retrieves cost value
        obs, switch_correct = self._next_observation(action)

        # Need to change this
        cost = random.randint(500, 3000)

        return obs, cost, switch_correct

    def render(self, mode='human', close=False):
        # Print some statements you want to print
        print(self.counter, "\t", self.reward, "\t", "\t", self.observation[0, :])

    def calculate_value(self, cost_agent_idx):
        value = (((1/cost_agent_idx) - (1/self.cost_initial))/((1/self.cost_idx_advisor) - (1/self.cost_initial)))
        return value


# Path of procedure
# First init in the test python file
# Then reset until some number of steps
# Inside reset set observation space and all the other variables
    # Proceed with the observation space received from reset and give it to the learning function to take action
    # Take that action and return the obs, reward, terminal and value states the value be used to compare
# If terminal is 1 then break from inner loop and go to outer loop
