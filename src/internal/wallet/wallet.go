package wallet

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mr-tron/base58"
)

const (
	WalletTypeOCF    = "ocf"
	WalletTypeSolana = "solana"

	defaultAccountsFile = "accounts.json"
	legacyWalletFile    = "wallet.json"
	accountsDirName     = "accounts"
)

type Account struct {
	Type      string    `json:"type"`
	PublicKey string    `json:"public_key"`
	Private   string    `json:"private_key"`
	FilePath  string    `json:"file_path"`
	CreatedAt time.Time `json:"created_at"`
}

type WalletManager struct {
	storageDir  string
	storagePath string
	accounts    []Account
}

func NewWalletManager() (*WalletManager, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("unable to determine home directory: %w", err)
	}

	baseDir := filepath.Join(homeDir, ".ocf")
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to ensure wallet directory: %w", err)
	}

	manager := &WalletManager{
		storageDir:  baseDir,
		storagePath: filepath.Join(baseDir, defaultAccountsFile),
	}

	if err := manager.loadAccounts(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (wm *WalletManager) loadAccounts() error {
	data, err := os.ReadFile(wm.storagePath)
	if errors.Is(err, os.ErrNotExist) {
		wm.accounts = []Account{}
		return wm.migrateLegacyWallet()
	}
	if err != nil {
		return fmt.Errorf("failed to read accounts file: %w", err)
	}

	var payload struct {
		Accounts []Account `json:"accounts"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("failed to parse accounts file: %w", err)
	}

	wm.accounts = payload.Accounts
	return nil
}

func (wm *WalletManager) migrateLegacyWallet() error {
	legacyPath := filepath.Join(wm.storageDir, legacyWalletFile)
	if _, err := os.Stat(legacyPath); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	data, err := os.ReadFile(legacyPath)
	if err != nil {
		return fmt.Errorf("failed to migrate legacy wallet: %w", err)
	}

	privateBytes, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return fmt.Errorf("failed to decode legacy wallet: %w", err)
	}
	if len(privateBytes) != ed25519.PrivateKeySize {
		return fmt.Errorf("legacy wallet has invalid size")
	}

	pub := ed25519.PrivateKey(privateBytes).Public().(ed25519.PublicKey)
	account := Account{
		Type:      WalletTypeOCF,
		PublicKey: base64.StdEncoding.EncodeToString(pub),
		Private:   base64.StdEncoding.EncodeToString(privateBytes),
		FilePath:  legacyPath,
		CreatedAt: time.Now().UTC(),
	}
	wm.accounts = append(wm.accounts, account)
	return wm.saveAccounts()
}

func (wm *WalletManager) saveAccounts() error {
	payload := struct {
		Accounts []Account `json:"accounts"`
	}{
		Accounts: wm.accounts,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal accounts: %w", err)
	}
	if err := os.WriteFile(wm.storagePath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write accounts file: %w", err)
	}
	return nil
}

func (wm *WalletManager) Accounts() []Account {
	out := make([]Account, len(wm.accounts))
	copy(out, wm.accounts)
	return out
}

func (wm *WalletManager) DefaultAccount() (Account, error) {
	if len(wm.accounts) == 0 {
		return Account{}, errors.New("no managed accounts")
	}
	return wm.accounts[0], nil
}

func (wm *WalletManager) AddSolanaAccount() (Account, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Account{}, fmt.Errorf("failed to generate Solana keypair: %w", err)
	}

	pub58 := base58.Encode(public)
	accountDir := filepath.Join(wm.storageDir, accountsDirName, pub58)
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		return Account{}, fmt.Errorf("failed to create account directory: %w", err)
	}

	keypairPath := filepath.Join(accountDir, "keypair.json")
	if err := writeSolanaKeypair(keypairPath, private); err != nil {
		return Account{}, err
	}

	account := Account{
		Type:      WalletTypeSolana,
		PublicKey: pub58,
		Private:   base64.StdEncoding.EncodeToString(private),
		FilePath:  keypairPath,
		CreatedAt: time.Now().UTC(),
	}
	wm.accounts = append(wm.accounts, account)
	if err := wm.saveAccounts(); err != nil {
		return Account{}, err
	}
	return account, nil
}

func writeSolanaKeypair(path string, private ed25519.PrivateKey) error {
	keyInts := make([]int, len(private))
	for i, b := range private {
		keyInts[i] = int(b)
	}
	data, err := json.MarshalIndent(keyInts, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode Solana keypair: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("failed to write Solana keypair: %w", err)
	}
	return nil
}

func (wm *WalletManager) FindByFile(path string) (Account, bool) {
	for _, acc := range wm.accounts {
		if acc.FilePath == path {
			return acc, true
		}
	}
	return Account{}, false
}

func (wm *WalletManager) WalletExists() bool {
	return len(wm.accounts) > 0
}

func (wm *WalletManager) GetPublicKey() string {
	if acc, err := wm.DefaultAccount(); err == nil {
		return acc.PublicKey
	}
	return ""
}

func (wm *WalletManager) GetPrivateKey() string {
	if acc, err := wm.DefaultAccount(); err == nil {
		return acc.Private
	}
	return ""
}

func (wm *WalletManager) GetWalletPath() string {
	if acc, err := wm.DefaultAccount(); err == nil {
		return acc.FilePath
	}
	return ""
}

func (wm *WalletManager) GetWalletType() string {
	if acc, err := wm.DefaultAccount(); err == nil {
		return acc.Type
	}
	return ""
}

func InitializeWallet() (*WalletManager, error) {
	wm, err := NewWalletManager()
	if err != nil {
		return nil, err
	}
	if !wm.WalletExists() {
		return nil, errors.New("no managed wallets found; run `ocf wallet create` to generate a Solana account")
	}
	return wm, nil
}
