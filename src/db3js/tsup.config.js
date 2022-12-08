import { defineConfig } from 'tsup'

export default defineConfig({
    entry: ['src/index.ts'],
    shims: true,
    format: ['esm', 'cjs', 'iife'],
})
