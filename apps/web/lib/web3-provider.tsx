'use client'

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createConfig, WagmiProvider } from 'wagmi'
import { http } from 'wagmi'
import { mainnet, polygon, arbitrum, optimism } from 'wagmi/chains'


const wagmiConfig = createConfig({
  chains: [mainnet, polygon, arbitrum, optimism],
  transports: {
    [mainnet.id]: http(),
    [polygon.id]: http(),
    [arbitrum.id]: http(),
    [optimism.id]: http(),
  },
})

const queryClient = new QueryClient()

export function Web3Provider({ children }: { children: React.ReactNode }) {
  return (
    <WagmiProvider config={wagmiConfig}>
      <QueryClientProvider client={queryClient}>
        {children}
      </QueryClientProvider>
    </WagmiProvider>
  )
}