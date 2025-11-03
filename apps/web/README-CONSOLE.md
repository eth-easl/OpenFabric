# OCF Web Console

A decentralized computing web console built on Next.js 15 with wallet authentication and machine management capabilities.

## Features

- **Wallet Authentication**: Connect with MetaMask, WalletConnect, and other Web3 wallets
- **Machine Management**: View connected computing resources and their status
- **API Key Management**: Generate and manage API keys for programmatic access
- **Real-time Status**: Monitor machine health, resource usage, and served models
- **Multi-chain Support**: Works with Ethereum, Polygon, Arbitrum, and Optimism
- **Responsive Design**: Built with Tailwind CSS and shadcn/ui components

## Setup Instructions

### 1. Install Dependencies

```bash
npm install
```

### 2. Environment Configuration

Create a `.env.local` file in the root directory:

```env
NEXT_PUBLIC_WALLETCONNECT_PROJECT_ID=your_walletconnect_project_id_here
NEXT_PUBLIC_API_BASE_URL=http://localhost:3000/api
```

To get a WalletConnect Project ID:
1. Visit [WalletConnect Cloud](https://cloud.walletconnect.com/)
2. Create a new project
3. Copy the Project ID

### 3. Run the Development Server

```bash
npm run dev
```

The application will be available at `http://localhost:3001`

## How It Works

### Authentication Flow
1. User clicks "Connect Wallet" in the navbar
2. Web3Modal opens with wallet options
3. User connects their wallet (MetaMask, WalletConnect, etc.)
4. Application switches to dashboard view

### Dashboard Features
- **User Profile**: Shows wallet address, ENS name (if available), and balance
- **Statistics**: Overview of total machines, active models, API keys, and requests
- **Machine Status**: Real-time status of connected computing resources
- **API Management**: Generate and manage API keys for accessing the OCF network

### API Endpoints

#### Get Machines
```
GET /api/machines?walletAddress=0x...
```

#### Get API Keys
```
GET /api/api-keys?walletAddress=0x...
```

#### Generate API Key
```
POST /api/api-keys
{
  "walletAddress": "0x...",
  "name": "API Key Name"
}
```

## Project Structure

```
apps/web/
├── app/
│   ├── api/
│   │   ├── machines/
│   │   └── api-keys/
│   ├── layout.tsx
│   └── page.tsx
├── components/
│   ├── dashboard.tsx
│   ├── wallet-connect.tsx
│   ├── ui/
│   └── navbar/
├── lib/
│   ├── web3-provider.tsx
│   └── utils.ts
└── package.json
```

## Tech Stack

- **Frontend**: Next.js 15, React 19, TypeScript
- **Styling**: Tailwind CSS, shadcn/ui
- **Web3**: Wagmi, Ethers.js, Web3Modal
- **State Management**: React Query
- **Authentication**: WalletConnect, MetaMask

## Development

### Available Scripts

```bash
npm run dev          # Start development server
npm run build        # Build for production
npm run start        # Start production server
npm run lint         # Run ESLint
```

### Building for Production

```bash
npm run build
npm start
```

## Future Enhancements

- Real-time machine status updates with WebSockets
- Machine registration and management
- Detailed usage analytics and billing
- Model deployment and management
- Advanced API key permissions and rate limiting
- Mobile app integration
- Multi-user support with role-based access control

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is part of the Open Compute Framework and is licensed under the MIT License.