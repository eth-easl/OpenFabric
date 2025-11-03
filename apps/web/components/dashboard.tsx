'use client'

import { useAccount, useBalance, useEnsName } from 'wagmi'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar'
import { formatEther } from 'ethers'
import { 
  Server, 
  Brain, 
  Key, 
  Activity, 
  Settings,
  Plus,
  RefreshCw
} from 'lucide-react'
import { useState, useEffect, useCallback } from 'react'

interface Machine {
  id: string
  name: string
  status: 'online' | 'offline' | 'busy'
  cpu: number
  memory: number
  gpu: number
  models: string[]
  lastSeen: string
}

interface ApiKey {
  id: string
  name: string
  key: string
  createdAt: string
  lastUsed: string | null
  requests: number
}

export function Dashboard() {
  const { address, isConnected, chain } = useAccount()
  const { data: ensName } = useEnsName({ address })
  const { data: balance } = useBalance({ address })
  
  const [machines, setMachines] = useState<Machine[]>([])
  const [apiKeys, setApiKeys] = useState<ApiKey[]>([])
  const [loading, setLoading] = useState(false)

  const fetchData = useCallback(() => {
    if (!address) return
    
    setLoading(true)
    
    // Mock data directly in the frontend
    const mockMachines: Machine[] = [
      {
        id: '1',
        name: 'GPU Server 1',
        status: 'online',
        cpu: 25,
        memory: 60,
        gpu: 45,
        models: ['Llama-2-7B', 'Stable Diffusion'],
        lastSeen: new Date(Date.now() - 2 * 60 * 1000).toISOString()
      },
      {
        id: '2',
        name: 'Edge Device A',
        status: 'busy',
        cpu: 85,
        memory: 90,
        gpu: 95,
        models: ['BERT-Base', 'GPT-3.5'],
        lastSeen: new Date(Date.now() - 1 * 60 * 1000).toISOString()
      },
      {
        id: '3',
        name: 'Cloud Node B',
        status: 'offline',
        cpu: 0,
        memory: 0,
        gpu: 0,
        models: [],
        lastSeen: new Date(Date.now() - 60 * 60 * 1000).toISOString()
      }
    ]

    const mockApiKeys: ApiKey[] = [
      {
        id: '1',
        name: 'Production Key',
        key: 'sk-...abc123',
        createdAt: '2024-01-15',
        lastUsed: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
        requests: 15420
      },
      {
        id: '2',
        name: 'Development Key',
        key: 'sk-...def456',
        createdAt: '2024-01-10',
        lastUsed: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
        requests: 892
      }
    ]

    setMachines(mockMachines)
    setApiKeys(mockApiKeys)
    setLoading(false)
  }, [address])

  useEffect(() => {
    if (isConnected && address) {
      fetchData()
    }
  }, [isConnected, address, fetchData])

  const generateApiKey = (name: string) => {
    if (!address) return
    
    const newApiKey: ApiKey = {
      id: Date.now().toString(),
      name,
      key: `sk-${Math.random().toString(36).substr(2, 9)}`,
      createdAt: new Date().toISOString(),
      lastUsed: null,
      requests: 0
    }

    setApiKeys(prev => [...prev, newApiKey])
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'online': return 'bg-green-500'
      case 'busy': return 'bg-yellow-500'
      case 'offline': return 'bg-red-500'
      default: return 'bg-gray-500'
    }
  }

  const getStatusText = (status: string) => {
    switch (status) {
      case 'online': return 'Online'
      case 'busy': return 'Busy'
      case 'offline': return 'Offline'
      default: return 'Unknown'
    }
  }

  const formatLastSeen = (lastSeen: string) => {
    const date = new Date(lastSeen)
    const now = new Date()
    const diff = now.getTime() - date.getTime()
    const minutes = Math.floor(diff / 60000)
    const hours = Math.floor(diff / 3600000)
    const days = Math.floor(diff / 86400000)

    if (minutes < 1) return 'Just now'
    if (minutes < 60) return `${minutes} minute${minutes > 1 ? 's' : ''} ago`
    if (hours < 24) return `${hours} hour${hours > 1 ? 's' : ''} ago`
    return `${days} day${days > 1 ? 's' : ''} ago`
  }

  if (!isConnected || !address) {
    return null
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="max-w-7xl mx-auto p-6 space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Avatar className="h-12 w-12">
              <AvatarImage src={`https://api.dicebear.com/7.x/identicon/svg?seed=${address}`} />
              <AvatarFallback>
                {address.slice(0, 2).toUpperCase()}
              </AvatarFallback>
            </Avatar>
            <div>
              <h1 className="text-2xl font-bold">
                Welcome, {ensName || `${address.slice(0, 6)}...${address.slice(-4)}`}
              </h1>
              <p className="text-muted-foreground">
                {chain?.name} Network
              </p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <div className="text-right">
              <div className="text-sm text-muted-foreground">Balance</div>
              <div className="text-lg font-semibold">
                {balance && `${Number(formatEther(balance.value)).toFixed(4)} ${balance.symbol}`}
              </div>
            </div>
            <Button variant="outline" size="sm" onClick={fetchData} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Machines</CardTitle>
              <Server className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{machines.length}</div>
              <p className="text-xs text-muted-foreground">
                {machines.filter(m => m.status === 'online').length} online
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Active Models</CardTitle>
              <Brain className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {machines.reduce((acc, machine) => acc + machine.models.length, 0)}
              </div>
              <p className="text-xs text-muted-foreground">
                Across all machines
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">API Keys</CardTitle>
              <Key className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{apiKeys.length}</div>
              <p className="text-xs text-muted-foreground">
                Active keys
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Requests</CardTitle>
              <Activity className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {apiKeys.reduce((acc, key) => acc + key.requests, 0).toLocaleString()}
              </div>
              <p className="text-xs text-muted-foreground">
                This month
              </p>
            </CardContent>
          </Card>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Machines Section */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>Connected Machines</CardTitle>
                  <CardDescription>Your computing resources and their status</CardDescription>
                </div>
                <Button size="sm">
                  <Plus className="h-4 w-4 mr-2" />
                  Add Machine
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {machines.map((machine) => (
                  <div key={machine.id} className="flex items-center justify-between p-4 border rounded-lg">
                    <div className="flex items-center gap-3">
                      <div className={`w-3 h-3 rounded-full ${getStatusColor(machine.status)}`} />
                      <div>
                        <div className="font-medium">{machine.name}</div>
                        <div className="text-sm text-muted-foreground">
                          {getStatusText(machine.status)} • {formatLastSeen(machine.lastSeen)}
                        </div>
                        <div className="flex gap-2 mt-1">
                          {machine.models.map((model) => (
                            <Badge key={model} variant="secondary" className="text-xs">
                              {model}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    </div>
                    <div className="text-right text-sm">
                      <div>CPU: {machine.cpu}%</div>
                      <div>RAM: {machine.memory}%</div>
                      <div>GPU: {machine.gpu}%</div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* API Keys Section */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>API Keys</CardTitle>
                  <CardDescription>Manage your API access keys</CardDescription>
                </div>
                <Button size="sm" onClick={() => {
                  const name = prompt('Enter a name for your API key:')
                  if (name) {
                    generateApiKey(name)
                  }
                }}>
                  <Plus className="h-4 w-4 mr-2" />
                  Generate Key
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {apiKeys.map((apiKey) => (
                  <div key={apiKey.id} className="flex items-center justify-between p-4 border rounded-lg">
                    <div>
                      <div className="font-medium">{apiKey.name}</div>
                      <div className="text-sm text-muted-foreground font-mono">
                        {apiKey.key}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Created: {new Date(apiKey.createdAt).toLocaleDateString()} • Last used: {apiKey.lastUsed ? formatLastSeen(apiKey.lastUsed) : 'Never'}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-medium">{apiKey.requests.toLocaleString()} requests</div>
                      <Button variant="outline" size="sm" className="mt-1">
                        <Settings className="h-3 w-3 mr-1" />
                        Manage
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}