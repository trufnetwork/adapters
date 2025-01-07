package main

import (
	"encoding/csv"
	"encoding/json"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

type Coin struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
	Image  string `json:"image"`
}

const (
	perPage    = 250
	totalCoins = 500
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}

func main() {
	if err := godotenv.Load(); err != nil {
		zap.L().Error("Error loading .env file", zap.Error(errors.WithStack(err)))
	}

	apiKey := os.Getenv("COINGECKO_API_KEY")
	if apiKey == "" {
		zap.L().Fatal("COINGECKO_API_KEY not set in .env file")
	}

	pages := (totalCoins + perPage - 1) / perPage
	var allCoins []Coin
	for page := 1; page <= pages; page++ {
		zap.L().Info("Fetching coins", zap.Int("page", page))
		url := "https://pro-api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=" + strconv.Itoa(perPage) + "&page=" + strconv.Itoa(page)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			zap.L().Error("Error creating HTTP request", zap.Error(errors.WithStack(err)))
			continue
		}

		req.Header.Add("accept", "application/json")
		req.Header.Add("x-cg-pro-api-key", apiKey)

		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			zap.L().Error("Error making HTTP request", zap.Error(errors.WithStack(err)))
			continue
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(res.Body)
			zap.L().Error("Non-OK HTTP status", zap.String("status", res.Status), zap.String("response_body", string(bodyBytes)))
			continue
		}

		body, err := io.ReadAll(res.Body)
		if err != nil {
			zap.L().Error("Error reading response body", zap.Error(errors.WithStack(err)))
			continue
		}

		var coins []Coin
		if err = json.Unmarshal(body, &coins); err != nil {
			zap.L().Error("Error unmarshalling JSON", zap.Error(errors.WithStack(err)))
			continue
		}

		allCoins = append(allCoins, coins...)
	}

	csvFile, err := os.Create("coins.csv")
	if err != nil {
		zap.L().Fatal("Error creating CSV file", zap.Error(errors.WithStack(err)))
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	if err = writer.Write([]string{"id", "symbol", "name", "image"}); err != nil {
		zap.L().Error("Error writing CSV headers", zap.Error(errors.WithStack(err)))
	}

	for _, coin := range allCoins {
		record := []string{coin.ID, coin.Symbol, coin.Name, coin.Image}
		if err = writer.Write(record); err != nil {
			zap.L().Error("Error writing record to CSV", zap.Error(errors.WithStack(err)))
		}
	}

	zap.L().Info("Successfully wrote coin data to coins.csv", zap.Int("total_coins", len(allCoins)))
}
