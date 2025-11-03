package solana

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/mr-tron/base58"
)

const (
	defaultRPCEndpoint = "https://api.mainnet-beta.solana.com"
)

type Client struct {
	endpoint   string
	httpClient *http.Client
}

func NewClient(endpoint string) *Client {
	if endpoint == "" {
		endpoint = defaultRPCEndpoint
	}
	return &Client{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *Client) HasSPLToken(ctx context.Context, owner string, mint string) (bool, error) {
	if _, err := base58.Decode(owner); err != nil {
		return false, fmt.Errorf("invalid owner public key: %w", err)
	}
	if _, err := base58.Decode(mint); err != nil {
		return false, fmt.Errorf("invalid mint address: %w", err)
	}

	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTokenAccountsByOwner",
		"params": []any{
			owner,
			map[string]string{
				"mint": mint,
			},
			map[string]any{
				"encoding": "jsonParsed",
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to query Solana RPC: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("solana rpc returned status %d", resp.StatusCode)
	}

	var rpcResp tokenAccountsResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return false, fmt.Errorf("failed to decode Solana RPC response: %w", err)
	}

	if rpcResp.Error != nil {
		return false, fmt.Errorf("solana rpc error (%d): %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	for _, entry := range rpcResp.Result.Value {
		amount := entry.Account.Data.Parsed.Info.TokenAmount.Amount
		if amount == "" {
			continue
		}
		if i, ok := new(big.Int).SetString(amount, 10); ok && i.Sign() > 0 {
			return true, nil
		}
	}

	return false, nil
}

type tokenAccountsResponse struct {
	Result struct {
		Value []struct {
			Account struct {
				Data struct {
					Parsed struct {
						Info struct {
							TokenAmount struct {
								Amount string `json:"amount"`
							} `json:"tokenAmount"`
						} `json:"info"`
					} `json:"parsed"`
				} `json:"data"`
			} `json:"account"`
		} `json:"value"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}
