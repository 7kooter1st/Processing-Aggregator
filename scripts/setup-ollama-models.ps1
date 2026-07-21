# Настройка Ollama: все модели в C:\OllamaModels
# Запускать от имени администратора для установки системной переменной.

$ModelsPath = "C:\OllamaModels"

Write-Host "=== Ollama models path setup ===" -ForegroundColor Cyan
Write-Host "Target: $ModelsPath"

if (-not (Test-Path $ModelsPath)) {
    New-Item -ItemType Directory -Path $ModelsPath -Force | Out-Null
    Write-Host "Created $ModelsPath"
}

# User + Machine (Machine требует админ)
[Environment]::SetEnvironmentVariable("OLLAMA_MODELS", $ModelsPath, "User")
Write-Host "OLLAMA_MODELS (User) = $ModelsPath"

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if ($isAdmin) {
    [Environment]::SetEnvironmentVariable("OLLAMA_MODELS", $ModelsPath, "Machine")
    Write-Host "OLLAMA_MODELS (Machine) = $ModelsPath" -ForegroundColor Green
} else {
    Write-Host "WARN: Run as Administrator to set Machine-level OLLAMA_MODELS" -ForegroundColor Yellow
    Write-Host "      Or run: setx OLLAMA_MODELS `"$ModelsPath`" /M" -ForegroundColor Yellow
}

# Остановить Ollama, чтобы подхватила новую переменную
Write-Host "`nStopping Ollama..."
Get-Process -Name "ollama*" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# Запуск с явной переменной (на случай если служба ещё не видит Machine env)
$env:OLLAMA_MODELS = $ModelsPath
Write-Host "Starting Ollama with OLLAMA_MODELS=$ModelsPath"
Start-Process "ollama" -ArgumentList "serve" -WindowStyle Hidden

Start-Sleep -Seconds 3
Write-Host "`nInstalled models:"
ollama list

Write-Host "`nTest run (Ctrl+C to exit interactive):"
Write-Host "  ollama run gemma4:26b"
Write-Host "`nProcessing Service .env:"
Write-Host "  OLLAMA_MODEL=gemma4:26b"
