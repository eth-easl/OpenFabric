'use client'

import { useConnect, useDisconnect, useAccount, useBalance, useEnsName } from 'wagmi'
import { Button } from '@/components/ui/button'
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar'
import { formatEther } from 'ethers'
import { useState, useEffect } from 'react'
import { injected } from 'wagmi/connectors'

export function WalletConnect() {
  const { connect, isPending } = useConnect()
  const { disconnect } = useDisconnect()
  const { address, isConnected, chain } = useAccount()
  const { data: ensName } = useEnsName({ address })
  const { data: balance } = useBalance({ address })
  const [isMounted, setIsMounted] = useState(false)

  useEffect(() => {
    setIsMounted(true)
  }, [])

  if (!isMounted) return null

  if (isConnected && address) {
    return (
      <div className="flex items-center gap-3">
        <div className="hidden sm:flex flex-col items-end">
          <div className="text-sm font-medium">
            {ensName || `${address.slice(0, 6)}...${address.slice(-4)}`}
          </div>
          <div className="text-xs text-muted-foreground">
            {balance && `${Number(formatEther(balance.value)).toFixed(4)} ${balance.symbol}`}
          </div>
          <div className="text-xs text-muted-foreground">
            {chain?.name}
          </div>
        </div>
        <Avatar className="h-8 w-8">
          <AvatarImage src={`https://api.dicebear.com/7.x/identicon/svg?seed=${address}`} />
          <AvatarFallback>
            {address.slice(0, 2).toUpperCase()}
          </AvatarFallback>
        </Avatar>
        <Button 
          variant="outline" 
          size="sm" 
          onClick={() => disconnect()}
        >
          Disconnect
        </Button>
      </div>
    )
  }

  return (
    <Button 
      onClick={() => connect({ connector: injected() })}
      disabled={isPending}
    >
      {isPending ? 'Connecting...' : 'Connect Wallet'}
    </Button>
  )
}