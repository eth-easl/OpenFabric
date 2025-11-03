'use client'

import { useAccount } from 'wagmi'
import Hero from '@/components/hero'
import { Dashboard } from '@/components/dashboard'

export default function Home() {
  const { isConnected } = useAccount()

  if (isConnected) {
    return <Dashboard />
  }

  return (
    <>
      <Hero />
    </>
  )
}