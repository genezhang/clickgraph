-- Teardown SQL for Social Benchmark Integration Tests
-- Drops all benchmark tables

DROP TABLE IF EXISTS brahmand.zeek_logs;
DROP TABLE IF EXISTS brahmand.friendships;
DROP TABLE IF EXISTS brahmand.post_likes_bench;
DROP TABLE IF EXISTS brahmand.posts_bench;
DROP TABLE IF EXISTS brahmand.user_follows_bench;
DROP TABLE IF EXISTS brahmand.users_bench;
