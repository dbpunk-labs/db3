//
// console.rs
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

use std::io::{stderr, Write};

use async_trait::async_trait;
use clap::Command;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::Parser;
use colored::Colorize;

use crate::command::{DB3ClientCommand, DB3ClientContext};
use crate::shell::{install_shell_plugins, AsyncHandler, CommandStructure, CompletionCache, Shell};
const DB3: &str = "
██████╗ ██████╗ ██████╗ 
██╔══██╗██╔══██╗╚════██╗
██║  ██║██████╔╝ █████╔╝
██║  ██║██╔══██╗ ╚═══██╗
██████╔╝██████╔╝██████╔╝
╚═════╝ ╚═════╝ ╚═════╝ 
@db3.network🚀🚀🚀";

#[derive(Parser)]
#[clap(name = "", rename_all = "kebab-case", no_binary_name = true)]
pub struct ConsoleOpts {
    #[clap(subcommand)]
    pub command: DB3ClientCommand,
}

pub async fn start_console(
    ctx: DB3ClientContext,
    out: &mut (dyn Write + Send),
    err: &mut (dyn Write + Send),
) -> Result<(), anyhow::Error> {
    writeln!(out, "{DB3}").unwrap();
    let app: Command = DB3ClientCommand::command();
    let mut shell = Shell::new(
        "db3>-$ ",
        ctx,
        ClientCommandHandler,
        CommandStructure::from_clap(&install_shell_plugins(app)),
    );
    shell.run_async(out, err).await
}

struct ClientCommandHandler;

#[async_trait]
impl AsyncHandler<DB3ClientContext> for ClientCommandHandler {
    async fn handle_async(
        &self,
        args: Vec<String>,
        context: &mut DB3ClientContext,
        completion_cache: CompletionCache,
    ) -> bool {
        match handle_command(get_command(args), context, completion_cache).await {
            Err(e) => {
                let _err = writeln!(stderr(), "{}", e.to_string().red());
                false
            }
            Ok(return_value) => return_value,
        }
    }
}

fn get_command(args: Vec<String>) -> Result<ConsoleOpts, anyhow::Error> {
    let app: Command = install_shell_plugins(ConsoleOpts::command());
    Ok(ConsoleOpts::from_arg_matches(
        &app.try_get_matches_from(args)?,
    )?)
}

async fn handle_command(
    opts: Result<ConsoleOpts, anyhow::Error>,
    ctx: &mut DB3ClientContext,
    _completion_cache: CompletionCache,
) -> Result<bool, anyhow::Error> {
    let opts = opts?;
    match opts.command.execute(ctx).await {
        Ok(table) => {
            table.printstd();
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }
    Ok(false)
}
