param(
    [Parameter(Mandatory = $true)]
    [string]$ImagePath
)

$ErrorActionPreference = 'Stop'
$gnBotsDir = 'C:\Program Files\GnBots'
$env:PATH = "$gnBotsDir;$gnBotsDir\x86;$env:PATH"
$env:TESSDATA_PREFIX = "$gnBotsDir\tessdata"

Add-Type -Path "$gnBotsDir\Tesseract.dll"
$engine = New-Object Tesseract.TesseractEngine(
    "$gnBotsDir\tessdata\",
    'eng',
    [Tesseract.EngineMode]::Default
)
$pix = [Tesseract.Pix]::LoadFromFile($ImagePath)
$page = $engine.Process($pix)
$iterator = $page.GetIterator()
$words = @()

try {
    $iterator.Begin()
    do {
        $text = $iterator.GetText([Tesseract.PageIteratorLevel]::Word)
        $rect = New-Object Tesseract.Rect
        if ($text -and $iterator.TryGetBoundingBox([Tesseract.PageIteratorLevel]::Word, [ref]$rect)) {
            $words += [pscustomobject]@{
                # OCR иногда возвращает кавычки и управляющие символы. В старом
                # Windows PowerShell 5.1 они могут превратить ConvertTo-Json в
                # невалидный JSON, поэтому текст передаётся как безопасный Base64.
                TextBase64 = [Convert]::ToBase64String(
                    [Text.Encoding]::UTF8.GetBytes($text.Trim())
                )
                X1 = $rect.X1
                Y1 = $rect.Y1
                X2 = $rect.X2
                Y2 = $rect.Y2
            }
        }
    } while ($iterator.Next([Tesseract.PageIteratorLevel]::Word))

    ConvertTo-Json -InputObject @($words) -Compress
}
finally {
    $iterator.Dispose()
    $page.Dispose()
    $pix.Dispose()
    $engine.Dispose()
}
