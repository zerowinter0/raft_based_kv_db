# 设置参数（修改为你的目录和参数）
$TargetDirectory = "D:\6.824\src\raft"  # 替换为你的项目目录
$Concurrency = 4                           # 并发数（如4）
$TotalRuns = 8                           # 总运行次数（如100）

# 初始化变量
$jobs = @()
$failedCount = 0
$failedOutputs = @()  # 新增：存储失败测试的完整输出

# 启动作业循环
for ($i = 1; $i -le $TotalRuns; $i++) {
    # 等待直到有空闲并发槽位
    while ($jobs.Count -ge $Concurrency) {
        Start-Sleep -Milliseconds 100
        # 移除已完成的作业并统计失败次数
        $completed = $jobs | Where-Object { $_.State -eq 'Completed' }
        foreach ($job in $completed) {
            # 获取作业的输出内容
            $output = Receive-Job -Job $job -ErrorAction SilentlyContinue
            # 检查输出是否包含"FAIL"
            if ($output -match "FAIL") {
                $failedCount++
                # 保存失败测试的完整输出（转换为字符串数组）
                $failedOutputs += ,@($output)  # 使用逗号强制为数组元素
            }
            # 清理作业
            $job | Remove-Job -Force
            $jobs = @($jobs | Where-Object { $_ -ne $job })
        }
    }

    # 启动新测试实例
    $job = Start-Job -ScriptBlock {
        param ($dir)
        Set-Location $dir
        go test -run 3A
    } -ArgumentList $TargetDirectory

    # 添加作业到数组（确保类型为数组）
    $jobs += $job
}

# 等待所有作业完成并统计剩余的失败次数
while ($jobs.Count -gt 0) {
    Start-Sleep -Milliseconds 100
    $completed = $jobs | Where-Object { $_.State -eq 'Completed' }
    foreach ($job in $completed) {
        # 获取作业的输出内容
        $output = Receive-Job -Job $job -ErrorAction SilentlyContinue
        # 检查输出是否包含"FAIL"
        if ($output -match "^\s*--- FAIL") {
            $failedCount++
            $failedOutputs += ,@($output)
        }
        # 清理作业
        $job | Remove-Job -Force
        $jobs = @($jobs | Where-Object { $_ -ne $job })
    }
}

# 输出结果
Write-Host "Total tests executed: $TotalRuns"
Write-Host "Failed tests: $failedCount"
Write-Host "Successful tests: $($TotalRuns - $failedCount)"
Write-Host "------------------------------"
Write-Host "Failed Test Outputs:"

# 打印所有失败测试的输出（逐个显示，用分隔符分隔）
foreach ($output in $failedOutputs) {
    Write-Host "------------------------------"
    foreach ($line in $output) {
        Write-Host $line
    }
    Write-Host "------------------------------"
}