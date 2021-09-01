package main

import (
	log "github.com/sirupsen/logrus"
	"main/src/internal/data/repository"
	"main/src/service"
	"os"
)

const (
	envRedisURI      = "REDIS_URI"
	envRedisUserName = "REDIS_USERNAME"
	envRedisPassword = "REDIS_PASS"
)

func main() {
	priceRepository := repository.NewPriceRepository(os.Getenv(envRedisURI), os.Getenv(envRedisUserName), os.Getenv(envRedisPassword), 0)
	if priceRepository == nil {
		log.WithFields(log.Fields{
			"file": "main.go",
		}).Fatal("Couldn't create price repository ")
	}

	priceSrv := service.NewPriceService(priceRepository)
	if priceSrv == nil {
		log.WithFields(log.Fields{
			"file": "main.go",
		}).Fatal("Couldn't create price service")
	}

	priceSrv.Start()
}
