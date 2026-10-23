#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
consumer_tmp=$(mktemp -d /tmp/rabbitmq-patch-consumer.XXXXXX)
trap 'rm -rf "$consumer_tmp"' EXIT

# A fresh cache prevents a previously packed version from masking package changes.
export NUGET_PACKAGES="$consumer_tmp/packages"
mkdir -p "$consumer_tmp/project"
dotnet msbuild "$repo_root/src/Osk.WolverineFx.RabbitMq.OAuth2/Osk.WolverineFx.RabbitMq.OAuth2.csproj" \
  -getProperty:PackageId,PackageVersion > "$consumer_tmp/package-properties.json"
python3 - "$repo_root" "$consumer_tmp" <<'PY'
import json, sys, zipfile
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent
from xml.etree.ElementTree import fromstring

repo, temporary = map(Path, sys.argv[1:])
properties = json.loads((temporary / 'package-properties.json').read_text())['Properties']
package = repo / 'artifacts' / f"{properties['PackageId']}.{properties['PackageVersion']}.nupkg"
with zipfile.ZipFile(package) as archive:
    required = [
        'lib/net9.0/Osk.WolverineFx.RabbitMq.OAuth2.xml',
        'lib/net10.0/Osk.WolverineFx.RabbitMq.OAuth2.xml',
        'analyzers/dotnet/cs/Osk.WolverineFx.RabbitMq.OAuth2.Generator.dll',
        'analyzers/dotnet/cs/Osk.WolverineFx.RabbitMq.OAuth2.Generator.xml',
    ]
    for path in required:
        content = archive.read(path)
        if path.endswith('.xml'):
            document = fromstring(content)
            assert document.find('members/member/summary') is not None, f'No XML documentation: {path}'
print('PASS: NuGet contains both runtime XML files and the generator DLL/XML.')
project = Element('Project', Sdk='Microsoft.NET.Sdk')
group = SubElement(project, 'PropertyGroup')
SubElement(group, 'OutputType').text = 'Exe'
SubElement(group, 'TargetFramework').text = 'net10.0'
SubElement(SubElement(project, 'ItemGroup'), 'PackageReference',
           Include=properties['PackageId'], Version=properties['PackageVersion'])
indent(project)
ElementTree(project).write(temporary / 'project/Consumer.csproj', encoding='unicode')

# Require our package to come from the artifact directory even if the same version
# has already been published. Resolve external dependencies from the public feed.
configuration = Element('configuration')
sources = SubElement(configuration, 'packageSources')
SubElement(sources, 'clear')
SubElement(sources, 'add', key='artifact', value=str(repo / 'artifacts'))
SubElement(sources, 'add', key='nuget', value='https://api.nuget.org/v3/index.json')
mapping = SubElement(configuration, 'packageSourceMapping')
SubElement(SubElement(mapping, 'packageSource', key='artifact'), 'package', pattern=properties['PackageId'])
SubElement(SubElement(mapping, 'packageSource', key='nuget'), 'package', pattern='*')
indent(configuration)
ElementTree(configuration).write(temporary / 'project/NuGet.Config', encoding='unicode')
PY
{
  printf '%s\n' 'using System;' 'using System.Linq;'
  cat "$repo_root/samples/StartupSmoke/Program.cs"
} > "$consumer_tmp/project/Program.cs"

dotnet restore "$consumer_tmp/project/Consumer.csproj" --configfile "$consumer_tmp/project/NuGet.Config"
dotnet run --project "$consumer_tmp/project/Consumer.csproj" -c Release --no-restore
