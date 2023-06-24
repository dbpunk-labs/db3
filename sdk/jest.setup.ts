import { TextEncoder, TextDecoder } from 'util'
globalThis.TextEncoder = TextEncoder
globalThis.TextDecoder = TextDecoder
import fetch, { Headers, Request, Response } from 'node-fetch'
if (!globalThis.fetch) {
    globalThis.fetch = fetch
    globalThis.Headers = Headers
    globalThis.Request = Request
    globalThis.Response = Response
}
class LocalStorageMock {
    constructor() {
        this.store = {}
    }

    clear() {
        this.store = {}
    }

    getItem(key) {
        return this.store[key] || null
    }

    setItem(key, value) {
        this.store[key] = String(value)
    }

    removeItem(key) {
        delete this.store[key]
    }
}

global.localStorage = new LocalStorageMock()
