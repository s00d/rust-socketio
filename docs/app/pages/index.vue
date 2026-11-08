<script setup lang="ts">
import type { ButtonProps, PageFeatureProps } from '@nuxt/ui'

const { data: page } = await useAsyncData('landing', () => queryCollection('landing').path('/').first())

useSeoMeta({
  title: 'socketio-next',
  description: page.value?.description,
  ogTitle: 'socketio-next',
  ogDescription: page.value?.description,
})

const heroLinks: ButtonProps[] = [
  { label: 'Get started', to: '/getting-started/introduction', size: 'lg', trailingIcon: 'i-lucide-arrow-right' },
  { label: 'Tauri plugin', to: '/tauri/setup', size: 'lg', color: 'neutral', variant: 'outline', icon: 'i-simple-icons-tauri' },
  { label: 'GitHub', to: 'https://github.com/s00d/rust-socketio', target: '_blank', size: 'lg', color: 'neutral', variant: 'outline', icon: 'i-simple-icons-github' },
]

const features: PageFeatureProps[] = [
  { title: 'socket.io-client 4.8 behaviour', description: 'Namespaces, auth, the same reconnection backoff, connection state recovery; checked against the reference client on the wire.', icon: 'i-lucide-check-check', to: '/getting-started/introduction' },
  { title: 'Acks both ways, binary anywhere', description: 'Awaitable acks with a timeout, answers to server callbacks, buffers anywhere in the arguments.', icon: 'i-lucide-arrow-left-right', to: '/client/events-and-acks' },
  { title: 'Typed state and errors', description: 'ConnectionState, DisconnectReason, LinkInfo and one cloneable Error; messages never contain URLs or query values.', icon: 'i-lucide-activity', to: '/client/connection-state' },
  { title: 'Polling, websocket, proxies, TLS', description: 'Upgrade or websocket only, HTTP proxies from the environment, rustls or native-tls, private CAs, OS trust store.', icon: 'i-lucide-shield-check', to: '/client/transports-and-proxies' },
  { title: 'Async or blocking', description: 'A tokio client with plain-closure or async handlers, and the same client without async behind a feature.', icon: 'i-lucide-cpu', to: '/client/blocking' },
  { title: 'Tauri 2 plugin', description: 'Named clients shared by Rust and the webviews, read-only clients, a URL allow-list and a JS API close to socket.io-client.', icon: 'i-simple-icons-tauri', to: '/tauri/setup' },
]

const quickLinks: ButtonProps[] = [
  { label: 'Quick start', to: '/getting-started/quick-start', trailingIcon: 'i-lucide-arrow-right' },
  { label: 'Migrate from rust_socketio', to: '/guides/migration', color: 'neutral', variant: 'outline' },
]
</script>

<template>
  <div>
    <UPageHero
      title="A Socket.IO v4 client for Rust"
      description="socketio-next behaves like the reference JavaScript client: namespaces, auth, acks in both directions, binary data, reconnection with the same backoff, polling and websocket transports. Async on tokio, with an optional blocking API and a Tauri 2 plugin."
      :links="heroLinks"
    >
      <template #headline>
        <UButton to="/changelog" label="socketio-next 0.1 is out" trailing-icon="i-lucide-arrow-right" color="neutral" variant="subtle" size="sm" />
      </template>
    </UPageHero>

    <UPageSection title="What you get" :features="features" />

    <UPageSection
      title="Connect, emit, await the ack"
      description="Add the crate, build a client with its handlers and connect. connect() resolves once the server accepted the namespace, or fails with the error of that attempt."
      orientation="horizontal"
      :links="quickLinks"
    >
      <div class="min-w-0">
        <ContentRenderer v-if="page" :value="page" />
      </div>
    </UPageSection>
  </div>
</template>
