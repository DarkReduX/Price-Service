// Package repository - contains implementation of redis repository with pricing
package repository

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"main/src/internal/data"
	"time"
)

// PriceRepository - repository data type
type PriceRepository struct {
	Client *redis.Client
}

func (r PriceRepository) SendNewPrice(price data.SymbolPrice) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	r.Client.Set(ctx, fmt.Sprintf("%d", price.Uuid), price, 0)
}

// NewPriceRepository - connect to cloud repository using environment variables
// REDIS_URI - url of redis database
// REDIS_USERNAME - database username
// REDIS_PASS - database user password
func NewPriceRepository(redisURI string, redisUsername string, redisUserPassword string, DB int) *PriceRepository {
	client := redis.NewClient(&redis.Options{
		Addr:     redisURI,
		Username: redisUsername,
		Password: redisUserPassword,
		DB:       DB,
	})
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	if client.Ping(ctx).Err() != nil {
		return nil
	}

	return &PriceRepository{Client: client}
}
