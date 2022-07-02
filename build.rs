//
//
// build.rs
// Copyright (C) 2022 db3.network
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

fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "proto/db3_base.proto",
                "proto/db3_memory_node.proto",
                "proto/db3_meta_node.proto",
                "proto/db3_compute_node.proto",
                "thirdparty/substrait/proto/substrait/plan.proto",
                "thirdparty/substrait/proto/substrait/type.proto",
                "thirdparty/substrait/proto/substrait/type_expressions.proto",
                "thirdparty/substrait/proto/substrait/parameterized_types.proto",
                "thirdparty/substrait/proto/substrait/function.proto",
                "thirdparty/substrait/proto/substrait/algebra.proto",
                "thirdparty/substrait/proto/substrait/capabilities.proto",
                "thirdparty/substrait/proto/substrait/extensions/extensions.proto",
            ],
            &["proto", "thirdparty/substrait/proto"],
        )
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {}
}
