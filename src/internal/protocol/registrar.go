package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"ocf/internal/common"
	"ocf/internal/platform"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/spf13/viper"
)

func RegisterLocalServices() {
	serviceName := viper.GetString("service.name")
	servicePort := viper.GetString("service.port")
	if serviceName == "llm" && servicePort != "" {
		// register the service by first fetch available models on the port
		err := healthCheckRemote(servicePort, 6000)
		if err != nil {
			common.Logger.Error("could not health check LLM service: ", err)
			return
		}
		common.Logger.Info("LLM service is healthy")
		registerLLMService(servicePort)
	}
}

func healthCheckRemote(port string, maxTries int) error {
	err := errors.New("initial error")
	tries := 0
	for err != nil {
		_, err := common.RemoteGET("http://localhost:" + port + "/health")
		if err != nil {
			common.Logger.Info("could not health check LLM service: ", err, " retrying in 10 seconds...")
			time.Sleep(10 * time.Second)
			tries++
		}
		if tries > maxTries {
			return err
		}
		if err == nil {
			break
		}
	}
	return nil
}

func registerLLMService(port string) {
	modelsBytes, err := common.RemoteGET("http://localhost:" + port + "/v1/models")
	if err != nil {
		common.Logger.Error("could not fetch models from LLM service: ", err)
	}
	common.Logger.Info("Fetched models from LLM service: ", string(modelsBytes))
	var availableModels common.LMAvailableModels
	err = json.Unmarshal(modelsBytes, &availableModels)
	if err != nil {
		common.Logger.Error("could not unmarshal models from LLM service: ", err)
	}
	var identityGroup []string
	for _, model := range availableModels.Models {
		identityGroup = append(identityGroup, "model="+model.Id)
	}

	// register the models
	service := Service{
		Name:          "llm",
		Status:        "connected",
		Host:          "localhost",
		Port:          port,
		IdentityGroup: identityGroup,
	}
	provideService(service)
}

func provideService(service Service) {
	host, _ := GetP2PNode(nil)
	ctx := context.Background()
	store, _ := GetCRDTStore()
	key := ds.NewKey(host.ID().String())
	myself.Service = []Service{service}
	if viper.GetString("public-addr") != "" {
		myself.PublicAddress = viper.GetString("public-addr")
	}
	common.Logger.Info("Registering LLM service: ", myself)
	value, err := json.Marshal(myself)
	common.ReportError(err, "Error while marshalling peer")
	store.Put(ctx, key, value)
}

func updateMyself() {
	store, _ := GetCRDTStore()
	ctx := context.Background()
	myself.Hardware.GPUs = platform.GetGPUInfo()
	value, err := json.Marshal(myself)
	key := ds.NewKey(myself.ID)
	common.ReportError(err, "Error while marshalling peer")
	err = store.Put(ctx, key, value)
	if err != nil {
		common.Logger.Warn("Error while updating myself in CRDT store: ", err)
	} else {
		common.Logger.Info("Updated myself in CRDT store: ", myself)
	}
}
