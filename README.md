# Osk.WolverineFx.RabbitMq.OAuth2.Patch

NuGet-библиотека для WolverineFx.RabbitMQ **6.49.0** и приложений .NET 9/10. Устанавливает два Harmony-патча автоматически при загрузке модуля приложения: после создания RabbitMQ-соединения запускает обновление OAuth2-токена, при остановке транспорта отменяет и ожидает фоновые задачи.

```xml
<PackageReference Include="Osk.WolverineFx.RabbitMq.OAuth2.Patch" Version="0.1.0" />
```

Для установки патчей достаточно этой ссылки на пакет в запускаемом C#-проекте. Дополнительные свойства MSBuild и вызовы регистрации не нужны. Source generator внутри пакета создаёт `ModuleInitializer`, вызывающий `RabbitMqOAuth2Patches.Install()` автоматически. Повторный вызов безопасен.

`EmitCompilerGeneratedFiles` лишь сохраняет сгенерированный код на диск для просмотра; для генерации и установки патчей это свойство не требуется. В примере оно отсутствует.

Настройте `ConnectionFactory.CredentialsProvider` вашим OAuth2-провайдером. Он должен возвращать непустой токен и `Credentials.ValidUntil` (оставшееся время жизни) больше 10 секунд. Обновление выполняется через `IConnection.UpdateSecretAsync`, с задержкой 60% TTL (не более 30 минут) и повторными попытками при сбоях. Обычные подключения с `BasicCredentialsProvider` пропускаются.

Пакет изменяет методы в процессе через Harmony. NativeAOT и trimming не поддерживаются. Генератор запускает регистрацию до кода модуля, но не перехватывает запуск произвольных процессов или уже открытые соединения. Другие версии Wolverine требуют отдельной проверки совместимости.

## Разработка

```sh
dotnet build src/Osk.WolverineFx.RabbitMq.OAuth2.Patch -c Release
dotnet test --project tests/Osk.WolverineFx.RabbitMq.OAuth2.Patch.Tests -c Release
dotnet pack src/Osk.WolverineFx.RabbitMq.OAuth2.Patch -c Release -o artifacts
bash tools/verify-package-consumer.sh
dotnet restore samples/StartupSmoke --source ./artifacts --source https://api.nuget.org/v3/index.json
dotnet run --project samples/StartupSmoke -c Release --no-restore
```

SDK и тестовый runner Microsoft Testing Platform закреплены в `global.json`; тесты используют TUnit. Настройки компилятора находятся в `Directory.Build.props`, версии зависимостей — в `Directory.Packages.props`. Они относятся к разработке этого репозитория. Потребителю не нужно копировать эти файлы.

`tools/verify-package-consumer.sh` создаёт отдельный проект вне репозитория и подключает собранный пакет обычной ссылкой `PackageReference`. Проверка использует новый NuGet-кэш и подтверждает установку обоих патчей до `Main`.

Пакет пока не опубликован в NuGet. Тесты используют подставные соединения; проверка с реальным RabbitMQ/OAuth2 требует брокера и провайдера токенов.
