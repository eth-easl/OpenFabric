package wallet

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewWalletManager(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Override the home directory for testing
	originalHome := os.Getenv("HOME")
	defer os.Setenv("HOME", originalHome)
	os.Setenv("HOME", tempDir)

	tests := []struct {
		name        string
		setup       func()
		expectError bool
	}{
		{
			name: "new wallet manager in clean directory",
			setup: func() {
				// Clean setup - no existing files
			},
			expectError: false,
		},
		{
			name: "existing accounts file",
			setup: func() {
				ocfDir := filepath.Join(tempDir, ".ocf")
				if err := os.MkdirAll(ocfDir, 0700); err != nil {
					t.Fatal(err)
				}

				accountsFile := filepath.Join(ocfDir, "accounts.json")
				accounts := struct {
					Accounts []Account `json:"accounts"`
				}{
					Accounts: []Account{
						{
							Type:      WalletTypeOCF,
							PublicKey: "test-public-key",
							Private:   "test-private-key",
							FilePath:  "test-path",
							CreatedAt: time.Now().UTC(),
						},
					},
				}
				data, err := json.MarshalIndent(accounts, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(accountsFile, data, 0600); err != nil {
					t.Fatal(err)
				}
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			wm, err := NewWalletManager()
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if wm.storageDir == "" {
				t.Error("storageDir should not be empty")
			}
			if wm.storagePath == "" {
				t.Error("storagePath should not be empty")
			}
			if wm.accounts == nil {
				t.Error("accounts should be initialized, not nil")
			}
		})
	}
}

func TestWalletManagerLoadAccounts(t *testing.T) {
	tempDir := t.TempDir()
	originalHome := os.Getenv("HOME")
	defer os.Setenv("HOME", originalHome)
	os.Setenv("HOME", tempDir)

	t.Run("load non-existent accounts file", func(t *testing.T) {
		wm := &WalletManager{
			storageDir:  filepath.Join(tempDir, ".ocf"),
			storagePath: filepath.Join(tempDir, ".ocf", "accounts.json"),
		}

		err := wm.loadAccounts()
		if err != nil {
			t.Errorf("Unexpected error loading non-existent file: %v", err)
		}
		if len(wm.accounts) != 0 {
			t.Errorf("Expected empty accounts, got %d", len(wm.accounts))
		}
	})

	t.Run("load valid accounts file", func(t *testing.T) {
		ocfDir := filepath.Join(tempDir, ".ocf")
		if err := os.MkdirAll(ocfDir, 0700); err != nil {
			t.Fatal(err)
		}

		accountsFile := filepath.Join(ocfDir, "accounts.json")
		accounts := struct {
			Accounts []Account `json:"accounts"`
		}{
			Accounts: []Account{
				{
					Type:      WalletTypeSolana,
					PublicKey: "test-solana-key",
					Private:   "test-private-key",
					FilePath:  "test-path",
					CreatedAt: time.Now().UTC(),
				},
			},
		}
		data, err := json.MarshalIndent(accounts, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(accountsFile, data, 0600); err != nil {
			t.Fatal(err)
		}

		wm := &WalletManager{
			storageDir:  ocfDir,
			storagePath: accountsFile,
		}

		err = wm.loadAccounts()
		if err != nil {
			t.Errorf("Unexpected error loading valid file: %v", err)
		}
		if len(wm.accounts) != 1 {
			t.Errorf("Expected 1 account, got %d", len(wm.accounts))
		}
		if wm.accounts[0].Type != WalletTypeSolana {
			t.Errorf("Expected account type %s, got %s", WalletTypeSolana, wm.accounts[0].Type)
		}
	})

	t.Run("load invalid JSON file", func(t *testing.T) {
		ocfDir := filepath.Join(tempDir, ".ocf")
		if err := os.MkdirAll(ocfDir, 0700); err != nil {
			t.Fatal(err)
		}

		accountsFile := filepath.Join(ocfDir, "accounts.json")
		if err := os.WriteFile(accountsFile, []byte("invalid json"), 0600); err != nil {
			t.Fatal(err)
		}

		wm := &WalletManager{
			storageDir:  ocfDir,
			storagePath: accountsFile,
		}

		err := wm.loadAccounts()
		if err == nil {
			t.Error("Expected error loading invalid JSON")
		}
	})
}

func TestWalletManagerMigrateLegacyWallet(t *testing.T) {
	tempDir := t.TempDir()
	originalHome := os.Getenv("HOME")
	defer os.Setenv("HOME", originalHome)
	os.Setenv("HOME", tempDir)

	t.Run("no legacy wallet to migrate", func(t *testing.T) {
		wm := &WalletManager{
			storageDir: filepath.Join(tempDir, ".ocf"),
		}

		err := wm.migrateLegacyWallet()
		if err != nil {
			t.Errorf("Unexpected error when no legacy wallet exists: %v", err)
		}
		if len(wm.accounts) != 0 {
			t.Errorf("Expected no accounts after migration attempt, got %d", len(wm.accounts))
		}
	})

	t.Run("migrate valid legacy wallet", func(t *testing.T) {
		ocfDir := filepath.Join(tempDir, ".ocf")
		if err := os.MkdirAll(ocfDir, 0700); err != nil {
			t.Fatal(err)
		}

		// Generate a valid Ed25519 keypair
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}

		legacyPath := filepath.Join(ocfDir, "wallet.json")
		privateEncoded := base64.StdEncoding.EncodeToString(private)
		if err := os.WriteFile(legacyPath, []byte(privateEncoded), 0600); err != nil {
			t.Fatal(err)
		}

		wm := &WalletManager{
			storageDir:  ocfDir,
			storagePath: filepath.Join(ocfDir, "accounts.json"),
		}

		err = wm.migrateLegacyWallet()
		if err != nil {
			t.Errorf("Unexpected error during migration: %v", err)
		}
		if len(wm.accounts) != 1 {
			t.Errorf("Expected 1 account after migration, got %d", len(wm.accounts))
		}

		account := wm.accounts[0]
		if account.Type != WalletTypeOCF {
			t.Errorf("Expected account type %s, got %s", WalletTypeOCF, account.Type)
		}
		if account.PublicKey != base64.StdEncoding.EncodeToString(public) {
			t.Error("Public key mismatch after migration")
		}
		if account.Private != privateEncoded {
			t.Error("Private key mismatch after migration")
		}
		if account.FilePath != legacyPath {
			t.Error("File path mismatch after migration")
		}
	})

	t.Run("migrate invalid legacy wallet - wrong size", func(t *testing.T) {
		ocfDir := filepath.Join(tempDir, ".ocf")
		if err := os.MkdirAll(ocfDir, 0700); err != nil {
			t.Fatal(err)
		}

		legacyPath := filepath.Join(ocfDir, "wallet.json")
		invalidKey := base64.StdEncoding.EncodeToString([]byte("too-short"))
		if err := os.WriteFile(legacyPath, []byte(invalidKey), 0600); err != nil {
			t.Fatal(err)
		}

		wm := &WalletManager{
			storageDir:  ocfDir,
			storagePath: filepath.Join(ocfDir, "accounts.json"),
		}

		err := wm.migrateLegacyWallet()
		if err == nil {
			t.Error("Expected error migrating invalid legacy wallet")
		}
	})
}

func TestWalletManagerSaveAccounts(t *testing.T) {
	tempDir := t.TempDir()

	wm := &WalletManager{
		storageDir:  tempDir,
		storagePath: filepath.Join(tempDir, "accounts.json"),
		accounts: []Account{
			{
				Type:      WalletTypeOCF,
				PublicKey: "test-public-key",
				Private:   "test-private-key",
				FilePath:  "test-path",
				CreatedAt: time.Now().UTC(),
			},
		},
	}

	err := wm.saveAccounts()
	if err != nil {
		t.Errorf("Unexpected error saving accounts: %v", err)
	}

	// Verify the file was created and contains correct data
	data, err := os.ReadFile(wm.storagePath)
	if err != nil {
		t.Errorf("Failed to read saved accounts file: %v", err)
	}

	var payload struct {
		Accounts []Account `json:"accounts"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Errorf("Failed to unmarshal saved accounts: %v", err)
	}

	if len(payload.Accounts) != 1 {
		t.Errorf("Expected 1 saved account, got %d", len(payload.Accounts))
	}
}

func TestWalletManagerAccounts(t *testing.T) {
	wm := &WalletManager{
		accounts: []Account{
			{Type: WalletTypeOCF, PublicKey: "key1"},
			{Type: WalletTypeSolana, PublicKey: "key2"},
		},
	}

	accounts := wm.Accounts()
	if len(accounts) != 2 {
		t.Errorf("Expected 2 accounts, got %d", len(accounts))
	}

	// Verify that modifying the returned slice doesn't affect the original
	accounts[0] = Account{Type: "modified"}
	if wm.accounts[0].Type == "modified" {
		t.Error("Modifying returned slice should not affect original")
	}
}

func TestWalletManagerDefaultAccount(t *testing.T) {
	tests := []struct {
		name        string
		accounts    []Account
		expectError bool
	}{
		{
			name:        "no accounts",
			accounts:    []Account{},
			expectError: true,
		},
		{
			name: "single account",
			accounts: []Account{
				{Type: WalletTypeOCF, PublicKey: "key1"},
			},
			expectError: false,
		},
		{
			name: "multiple accounts",
			accounts: []Account{
				{Type: WalletTypeOCF, PublicKey: "key1"},
				{Type: WalletTypeSolana, PublicKey: "key2"},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wm := &WalletManager{accounts: tt.accounts}

			account, err := wm.DefaultAccount()
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if account != tt.accounts[0] {
				t.Error("Default account should be the first one")
			}
		})
	}
}

func TestWalletManagerAddSolanaAccount(t *testing.T) {
	tempDir := t.TempDir()
	originalHome := os.Getenv("HOME")
	defer os.Setenv("HOME", originalHome)
	os.Setenv("HOME", tempDir)

	// Create the wallet manager
	ocfDir := filepath.Join(tempDir, ".ocf")
	if err := os.MkdirAll(ocfDir, 0700); err != nil {
		t.Fatal(err)
	}

	wm := &WalletManager{
		storageDir:  ocfDir,
		storagePath: filepath.Join(ocfDir, "accounts.json"),
		accounts:    []Account{},
	}

	account, err := wm.AddSolanaAccount()
	if err != nil {
		t.Errorf("Unexpected error adding Solana account: %v", err)
	}

	// Verify account properties
	if account.Type != WalletTypeSolana {
		t.Errorf("Expected account type %s, got %s", WalletTypeSolana, account.Type)
	}
	if account.PublicKey == "" {
		t.Error("Public key should not be empty")
	}
	if account.Private == "" {
		t.Error("Private key should not be empty")
	}
	if account.FilePath == "" {
		t.Error("File path should not be empty")
	}
	if account.CreatedAt.IsZero() {
		t.Error("Created at should not be zero")
	}

	// Verify account was added to manager
	if len(wm.accounts) != 1 {
		t.Errorf("Expected 1 account, got %d", len(wm.accounts))
	}

	// Verify keypair file was created
	if _, err := os.Stat(account.FilePath); os.IsNotExist(err) {
		t.Error("Keypair file should exist")
	}

	// Verify the saved keypair can be loaded
	data, err := os.ReadFile(account.FilePath)
	if err != nil {
		t.Errorf("Failed to read keypair file: %v", err)
	}

	var keyInts []int
	if err := json.Unmarshal(data, &keyInts); err != nil {
		t.Errorf("Failed to unmarshal keypair: %v", err)
	}

	if len(keyInts) != ed25519.PrivateKeySize {
		t.Errorf("Expected key size %d, got %d", ed25519.PrivateKeySize, len(keyInts))
	}
}

func TestWriteSolanaKeypair(t *testing.T) {
	tempDir := t.TempDir()
	keypairPath := filepath.Join(tempDir, "keypair.json")

	// Generate a test keypair
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	err = writeSolanaKeypair(keypairPath, private)
	if err != nil {
		t.Errorf("Unexpected error writing keypair: %v", err)
	}

	// Verify file exists and has correct permissions
	info, err := os.Stat(keypairPath)
	if err != nil {
		t.Errorf("Failed to stat keypair file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("Expected file permissions 0600, got %o", info.Mode().Perm())
	}

	// Verify content
	data, err := os.ReadFile(keypairPath)
	if err != nil {
		t.Errorf("Failed to read keypair file: %v", err)
	}

	var keyInts []int
	if err := json.Unmarshal(data, &keyInts); err != nil {
		t.Errorf("Failed to unmarshal keypair: %v", err)
	}

	if len(keyInts) != ed25519.PrivateKeySize {
		t.Errorf("Expected key size %d, got %d", ed25519.PrivateKeySize, len(keyInts))
	}

	// Verify the decoded key matches the original
	reconstructed := make([]byte, len(keyInts))
	for i, keyInt := range keyInts {
		reconstructed[i] = byte(keyInt)
	}

	for i := range private {
		if reconstructed[i] != private[i] {
			t.Errorf("Key mismatch at position %d", i)
		}
	}
}

func TestWalletManagerFindByFile(t *testing.T) {
	wm := &WalletManager{
		accounts: []Account{
			{Type: WalletTypeOCF, PublicKey: "key1", FilePath: "/path/to/file1"},
			{Type: WalletTypeSolana, PublicKey: "key2", FilePath: "/path/to/file2"},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "existing file path",
			path:     "/path/to/file1",
			expected: true,
		},
		{
			name:     "non-existing file path",
			path:     "/path/to/nonexistent",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, found := wm.FindByFile(tt.path)
			if found != tt.expected {
				t.Errorf("FindByFile(%s) = %v, want %v", tt.path, found, tt.expected)
			}
		})
	}
}

func TestWalletManagerWalletExists(t *testing.T) {
	tests := []struct {
		name     string
		accounts []Account
		expected bool
	}{
		{
			name:     "no accounts",
			accounts: []Account{},
			expected: false,
		},
		{
			name:     "has accounts",
			accounts: []Account{{Type: WalletTypeOCF}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wm := &WalletManager{accounts: tt.accounts}
			exists := wm.WalletExists()
			if exists != tt.expected {
				t.Errorf("WalletExists() = %v, want %v", exists, tt.expected)
			}
		})
	}
}

func TestWalletManagerGetMethods(t *testing.T) {
	wm := &WalletManager{
		accounts: []Account{
			{
				Type:      WalletTypeOCF,
				PublicKey: "test-public-key",
				Private:   "test-private-key",
				FilePath:  "/test/path",
			},
		},
	}

	if wm.GetPublicKey() != "test-public-key" {
		t.Errorf("GetPublicKey() = %v, want %v", wm.GetPublicKey(), "test-public-key")
	}

	if wm.GetPrivateKey() != "test-private-key" {
		t.Errorf("GetPrivateKey() = %v, want %v", wm.GetPrivateKey(), "test-private-key")
	}

	if wm.GetWalletPath() != "/test/path" {
		t.Errorf("GetWalletPath() = %v, want %v", wm.GetWalletPath(), "/test/path")
	}

	if wm.GetWalletType() != WalletTypeOCF {
		t.Errorf("GetWalletType() = %v, want %v", wm.GetWalletType(), WalletTypeOCF)
	}

	// Test with empty accounts
	emptyWm := &WalletManager{accounts: []Account{}}

	if emptyWm.GetPublicKey() != "" {
		t.Error("GetPublicKey() should return empty string when no accounts")
	}

	if emptyWm.GetPrivateKey() != "" {
		t.Error("GetPrivateKey() should return empty string when no accounts")
	}

	if emptyWm.GetWalletPath() != "" {
		t.Error("GetWalletPath() should return empty string when no accounts")
	}

	if emptyWm.GetWalletType() != "" {
		t.Error("GetWalletType() should return empty string when no accounts")
	}
}

func TestInitializeWallet(t *testing.T) {
	tempDir := t.TempDir()
	originalHome := os.Getenv("HOME")
	defer os.Setenv("HOME", originalHome)
	os.Setenv("HOME", tempDir)

	t.Run("initialize wallet with existing accounts", func(t *testing.T) {
		ocfDir := filepath.Join(tempDir, ".ocf")
		if err := os.MkdirAll(ocfDir, 0700); err != nil {
			t.Fatal(err)
		}

		accountsFile := filepath.Join(ocfDir, "accounts.json")
		accounts := struct {
			Accounts []Account `json:"accounts"`
		}{
			Accounts: []Account{
				{
					Type:      WalletTypeOCF,
					PublicKey: "test-key",
					Private:   "test-private",
					FilePath:  "test-path",
					CreatedAt: time.Now().UTC(),
				},
			},
		}
		data, err := json.MarshalIndent(accounts, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(accountsFile, data, 0600); err != nil {
			t.Fatal(err)
		}

		wm, err := InitializeWallet()
		if err != nil {
			t.Errorf("Unexpected error initializing wallet: %v", err)
		}
		if !wm.WalletExists() {
			t.Error("Wallet should exist after initialization")
		}
	})

	t.Run("initialize wallet without existing accounts", func(t *testing.T) {
		// Use a different temp directory to ensure clean state
		tempDir2 := t.TempDir()
		os.Setenv("HOME", tempDir2)

		wm, err := InitializeWallet()
		if err == nil {
			t.Error("Expected error when no managed wallets found")
		}
		if wm != nil {
			t.Error("Wallet manager should be nil when initialization fails")
		}
	})
}