#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
consumer_tmp=$(mktemp -d /tmp/rabbitmq-patch-consumer.XXXXXX)
trap 'rm -rf "$consumer_tmp"' EXIT

# A fresh cache prevents a previously packed 0.1.0 from masking package changes.
export NUGET_PACKAGES="$consumer_tmp/packages"
mkdir -p "$consumer_tmp/project"
cat > "$consumer_tmp/project/Consumer.csproj" <<'XML'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Osk.WolverineFx.RabbitMq.OAuth2.Patch" Version="0.1.0" />
  </ItemGroup>
</Project>
XML
{
  printf '%s\n' 'using System;' 'using System.Linq;'
  cat "$repo_root/samples/StartupSmoke/Program.cs"
} > "$consumer_tmp/project/Program.cs"

dotnet restore "$consumer_tmp/project/Consumer.csproj" --source "$repo_root/artifacts" --source https://api.nuget.org/v3/index.json
dotnet run --project "$consumer_tmp/project/Consumer.csproj" -c Release --no-restore
