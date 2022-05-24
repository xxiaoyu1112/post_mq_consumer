package redis

import Redis "github.com/go-redis/redis/v8"

// init redis
var rdb *Redis.Client = Redis.NewClient(&Redis.Options{
	Addr: "211.71.76.189:6379",
	DB:   0, // use default DB
})
