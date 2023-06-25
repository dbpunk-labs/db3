//
// readable.ts
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

export function bytesToReadableNum(bytes_size: number): string {
    const STORAGE_LABELS: string[] = [' ', 'K', 'M', 'G', 'T', 'P', 'E']
    const max_shift = 7
    var shift = 0
    var local_bytes_size = bytes_size
    var value = bytes_size
    local_bytes_size >>= 10
    while (local_bytes_size > 0 && shift < max_shift) {
        value /= 1024.0
        shift += 1
        local_bytes_size >>= 10
    }
    return value.toFixed(2) + STORAGE_LABELS[shift]
}

export function unitsToReadableNum(units: number): string {
    return (units / 1000_000_000.0).toFixed(6)
}
