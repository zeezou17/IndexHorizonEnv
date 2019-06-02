
from gym.envs.registration import register

register(
    id='postgres-idx-advisor-v0',
    entry_point='gym.envs.postgres_idx_advisor.envs.postgres_idx_advisor_env:PostgresIdxAdvisorEnv',
    max_episode_steps=200,
    reward_threshold=100.0,
)
#register(
#    id='foo-extrahard-v0',
#    entry_point='gym_foo.envs:FooExtraHardEnv',
#    timestep_limit=1000,
#)