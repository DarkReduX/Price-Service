package service

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"main/src/internal/data"
	"main/src/internal/data/repository"
	pb "main/src/protocol"
	"net"
	"strings"
)

type Price struct {
	Repository *repository.PriceRepository
	pb.UnimplementedPriceServiceServer
	symbolsPrice map[string]data.SymbolPrice
	streams      []pb.PriceService_SendPriceServer
	posStreams   map[string]pb.PriceService_SendPosNewPriceServer
}

func (s *Price) Start() {
	lis, err := net.Listen("tcp", "localhost:8081")

	if err != nil {
		log.WithFields(log.Fields{
			"file":  "priceUpdateConsumer.go",
			"func":  "Start()",
			"error": err,
		}).Fatal("Failed to listen on port 8081 while starting price service")
	}

	go s.GetUpdatePricesFromStream()

	grpcServer := grpc.NewServer()
	pb.RegisterPriceServiceServer(grpcServer, s)
	if err = grpcServer.Serve(lis); err != nil {
		log.WithFields(log.Fields{
			"file":  "priceUpdateConsumer.go",
			"func":  "Start()",
			"error": err,
		}).Fatalf("Failed to server gRPC server over port 8081 while starting price service")
	}
}

func (s *Price) GetUpdatePricesFromStream() {
	for {
		priceBytes := make([]byte, 1000)
		price := data.SymbolPrice{}
		if err := s.Repository.Client.RPop(context.Background(), "queueKey").Scan(&priceBytes); err != nil {
			continue
		}
		if err := json.Unmarshal(priceBytes, &price); err != nil {
			log.WithFields(log.Fields{
				"service": "price",
				"error":   err,
			}).Errorf("Couldn't unmarshall price")
		}
		if price.Uuid == 0 {
			continue
		}

		go func() {
			for k, stream := range s.posStreams {
				symbol := strings.Split(k, "-")[1]
				if price.Symbol == symbol {
					err := stream.Send(&pb.Price{
						Uuid:   price.Uuid,
						Symbol: price.Symbol,
						Bid:    price.Bid,
						Ask:    price.Ask,
					})

					if err != nil {
						delete(s.posStreams, k)
						return
					}
				}
			}
		}()

		for i, stream := range s.streams {
			if stream == nil {
				continue
			}
			if err := stream.Send(&pb.Price{
				Uuid:   price.Uuid,
				Symbol: price.Symbol,
				Bid:    price.Bid,
				Ask:    price.Ask,
			}); err != nil {
				s.streams[i] = s.streams[len(s.streams)-1]
				s.streams[len(s.streams)-1] = nil
				s.streams = s.streams[:len(s.streams)-1]
			}
		}
		log.WithFields(log.Fields{
			"from service": "random price service",
			"service":      "price",
			"price symbol": price.Symbol,
		}).Info("Received price")

		s.symbolsPrice[price.Symbol] = price
	}
}

func (s *Price) SendPrice(_ *pb.Conn, stream pb.PriceService_SendPriceServer) error {
	if s.streams == nil {
		s.streams = make([]pb.PriceService_SendPriceServer, 0, 10)
	}

	log.WithFields(log.Fields{
		"service": "price",
		"stream":  "send price",
	}).Info("Connected client to service")

	s.streams = append(s.streams, stream)

	for _, v := range s.symbolsPrice {
		if err := stream.Send(&pb.Price{
			Uuid:   v.Uuid,
			Symbol: v.Symbol,
			Bid:    v.Bid,
			Ask:    v.Ask,
		}); err != nil {
			return err
		}
	}

	for {
		ch := make(chan error)
		ch <- stream.Context().Err()
		select {
		case <-ch:
			return stream.Context().Err()
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *Price) ValidateSymbolPrice(_ context.Context, price *pb.Price) (*pb.ValidResponse, error) {
	log.WithFields(log.Fields{
		"service":      "price",
		"price symbol": price.Symbol,
	}).Info("Received request to validate symbol price")

	if symbolPrice, ok := s.symbolsPrice[price.Symbol]; ok && symbolPrice.Uuid == price.Uuid && symbolPrice.Ask == price.Ask && symbolPrice.Bid == price.Bid {
		return &pb.ValidResponse{IsValid: true}, nil
	}
	return &pb.ValidResponse{IsValid: false}, nil
}

func (s *Price) SendPosNewPrice(c *pb.Conn, stream pb.PriceService_SendPosNewPriceServer) error {
	s.posStreams[c.Message] = stream

	log.WithFields(log.Fields{
		"service": "price",
		"stream":  "send new pos price",
	}).Info("Connected client to service")

	for {
		ch := make(chan error)
		ch <- stream.Context().Err()
		select {
		case <-ch:
			return stream.Context().Err()
		case <-stream.Context().Done():
			return nil
		}
	}
}

func NewPriceService(priceRepository *repository.PriceRepository) *Price {
	return &Price{
		Repository:                      priceRepository,
		UnimplementedPriceServiceServer: pb.UnimplementedPriceServiceServer{},
		symbolsPrice:                    make(map[string]data.SymbolPrice),
		streams:                         make([]pb.PriceService_SendPriceServer, 0, 10),
		posStreams:                      make(map[string]pb.PriceService_SendPosNewPriceServer),
	}
}
