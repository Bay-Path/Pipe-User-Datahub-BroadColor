## Stage 1: Get Credentials stored in vault
$vaultHeaders = @{}
$vaultHeaders.Add('Authorization','Bearer '+ $env:vaultToken)
$vaultHeaders.Add('Content-Type', 'application/json')
$vaultRepo = "dept-ait"
$vaultPath = "pipe-user-datahub-broad-color"
$vaultURL = "https://vault.it.baypath.edu/v1/$($vaultRepo)/data/$($vaultPath)"
$vault = (Invoke-RestMethod -Uri $vaultURL -Method Get -Headers $VaultHeaders).data.data

#and set Broad Color Headers
$colorHeaders = @{
    Authorization = "Bearer $($Vault.colorToken)"
}

## Stage 2: Get list of Employees from BPU-FACSTAFF Mailing List View
Write-Output "[INFO] Gathering data from Datahub for faculty & staff members"
$facstaffDataTable = New-Object System.Data.DataTable
$sqlConnection = New-Object Data.SQLClient.SQLConnection "Server=$($vault.jenzabarURI);database=$($vault.jenzabarDatabase);trusted_connection=false;User ID=$($vault.jenzabarUsername);Password=$($vault.jenzabarPassword)"
$sqlQuery = "SELECT [ID_NUM] FROM Automation.dbo.BPM_EMAIL_FACSTAFF"
$sqlConnection.open()
$SQLCommand = $sqlConnection.CreateCommand()
$sqlCommand.CommandText = $sqlQuery
$sqlReader = $sqlCommand.ExecuteReader()
$facstaffDataTable.Load($sqlReader)
$sqlConnection.close()
$facstaffDataTable = @($facstaffDataTable)
Write-Output "[INFO] Data gathered. Records: $($facstaffDataTable.count)"
## Get list of Students from BPU-Students Mailing List View
Write-Output "[INFO] Gathering data from Datahub for students"
$studentDataTable = New-Object System.Data.DataTable
$sqlConnection = New-Object Data.SQLClient.SQLConnection "Server=$($vault.jenzabarURI);database=$($vault.jenzabarDatabase);trusted_connection=false;User ID=$($vault.jenzabarUsername);Password=$($vault.jenzabarPassword)"
$sqlQuery = "SELECT [ID_NUM] FROM Automation.dbo.BPM_EMAIL_STUDENTS_ALL"
$sqlConnection.open()
$SQLCommand = $sqlConnection.CreateCommand()
$sqlCommand.CommandText = $sqlQuery
$sqlReader = $sqlCommand.ExecuteReader()
$studentDataTable.Load($sqlReader)
$sqlConnection.close()
$studentDataTable = @($studentDataTable)
Write-Output "[INFO] Data gathered. Records: $($studentDataTable.count)"

## Handle staff first
Write-Output "[INFO] Processing faculty and staff records"
Foreach ($user in $facstaffDataTable[6]){
    #Lookup user in Active Directory to make sure we have the latest email address for the user
    $ADUser = get-aduser -ldapfilter "(employeeid=$($user.id_num))" -Properties mail | Select -ExpandProperty mail
        if ($ADUser -eq $null){
            Write-Output "[WARNING] unable to find $($user.id_num) in Active Directory"
        }
        if ($ADUser -ne $null){
            Write-Output "[INFO] Successfully found $($user.id_num) in Active Directory. Email: $($ADUser)"
            ## Check to see if they have already been added to color
            Write-Output "[INFO] Checking to see if they are marked eligible in Color"
            $colorURI = "https://api.color.com/api/v1/external/eligibility/entries?unique_identifiers=$($ADUser)"
            $colorEligibilityCheck = Invoke-RestMethod -Headers $colorHeaders -Uri $colorUri -Method GET
            ## Are already in Color
            If ($colorEligibilityCheck.results.id.length -ge 1) {
                Write-Output "[INFO] $($ADUser) is marked eligible in Color"
                ## Check to see if they are already in the datahub
                $datahubCheckDataTable = New-Object System.Data.DataTable
                $sqlConnection = New-Object Data.SQLClient.SQLConnection "Server=$($vault.datahubURI);database=$($vault.datahubDatabase);trusted_connection=false;User ID=$($vault.datahubUsername);Password=$($vault.datahubPassword)"
                $sqlQuery = "SELECT [unique_identifier] FROM [BroadColor].[raw].[eligibility] WHERE [unique_identifier] like '$($ADUser)%'"
                $sqlConnection.open()
                $SQLCommand = $sqlConnection.CreateCommand()
                $sqlCommand.CommandText = $sqlQuery
                $sqlReader = $sqlCommand.ExecuteReader()
                $datahubCheckDataTable.Load($sqlReader)
                $sqlConnection.close()
                $datahubCheckDataTable = @($datahubCheckDataTable)
                If ($datahubCheckDataTable.length -ge 1){
                    Write-Output "[INFO] $($ADuser) entered into Datahub"
                }
                If ($datahubCheckDataTable.length -eq 0){
                    Write-Output "[WARNING] $($ADUser) does not exist in Datahub"
                }
            }
            ## Are not in Color
            If ($colorEligibilityCheck.results.id.length -eq 0){
                Write-Output "[INFO] $($ADUser) is not marked eligible in Color."
                $colorURI = "https://api.color.com/api/v1/external/eligibility/entries"
                $Payload = @{
                    unique_identifier = $($ADUser)
                    identifier_type = 'email'
                    population = 'Bay Path - Employees'
                    external_id = $($ADUser)
                }
                $colorEligibilityAdd = Invoke-RestMethod -Headers $colorHeaders -Uri $colorURI -Method POST -Body $Payload
                Write-Output "[INFO] Re-checking eligibility"
                $colorURI = "https://api.color.com/api/v1/external/eligibility/entries?unique_identifiers=$($ADUser)"
                $colorEligibilityCheck = Invoke-RestMethod -Headers $colorHeaders -Uri $colorUri -Method GET
                If ($colorEligibilityCheck.results.id.length -ge 1) {
                    Write-Output "[INFO] $($ADUser) is marked eligible in Color. Color ID# is $($colorEligibilityCheck.results.id)"
                }
                ##Add to Datahub
                $sqlCommand = New-Object System.Data.SqlClient.SqlCommand
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@id",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@unique_identifier",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@population",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@external_id",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@created_at",[Data.SQLDBType]::datetime))) | Out-Null
                $SqlCommand.Parameters["@id"].Value = $colorEligibilityCheck.results.id
                $SqlCommand.Parameters["@unique_identifier"].Value = $colorEligibilityCheck.results.unique_identifier
                $SqlCommand.Parameters["@population"].Value = $colorEligibilityCheck.results.population
                $SqlCommand.Parameters["@external_id"].Value = $colorEligibilityCheck.results.external_id
                $SqlCommand.Parameters["@created_at"].Value = (Get-Date -Format "yyyy-MM-dd hh:mm:ss")
                $sqlQuery = "
                BEGIN TRAN
                    IF EXISTS (SELECT [id],[unique_identifier],[population],[external_id],[created_at] FROM [$($vault.datahubDatabase)].[raw].[eligibility] WHERE [id] = @id) 
                        UPDATE [$($vault.datahubDatabase)].[raw].[eligibility]
                            SET [unique_identifier] = @unique_identifier,
                                [population] = @population,
                                [external_id] = @external_id,
                                [created_at] = @created_at
                        WHERE [id] = @id;
                    ELSE
                        INSERT INTO [$($vault.datahubDatabase)].[raw].[eligibility] ([id],[unique_identifier],[population],[external_id],[created_at]) 
                        VALUES (@id,@unique_identifier,@population,@external_id,@created_at)
                COMMIT
                "
                $sqlConnection = New-Object Data.SQLClient.SQLConnection "Server=$($vault.datahubURI);database=$($vault.datahubDatabase);trusted_connection=false;User ID=$($vault.datahubUsername);Password=$($vault.datahubPassword)"
                $sqlConnection.open()
                $sqlCommand.CommandText = $sqlQuery
                $sqlCommand.Connection = $sqlConnection
                $sqlCMDStatus = $sqlCommand.ExecuteNonQuery()
                $sqlConnection.close()
            }
            
        }
}
Write-Output "[INFO] faculty and staff records complete"

## Now do students!
Write-Output "[INFO] Processing student records"
Foreach ($user in $studentDataTable[2]){
    #Lookup user in Active Directory to make sure we have the latest email address for the user
    $ADUser = get-aduser -ldapfilter "(employeeid=$($user.id_num))" -Properties mail | Select -ExpandProperty mail
        if ($ADUser -eq $null){
            Write-Output "[WARNING] unable to find $($user.id_num) in Active Directory"
        }
        if ($ADUser -ne $null){
            Write-Output "[INFO] Successfully found $($user.id_num) in Active Directory. Email: $($ADUser)"
            ## Check to see if they have already been added to color
            Write-Output "[INFO] Checking to see if they are marked eligible in Color"
            $colorURI = "https://api.color.com/api/v1/external/eligibility/entries?unique_identifiers=$($ADUser)"
            $colorEligibilityCheck = Invoke-RestMethod -Headers $colorHeaders -Uri $colorUri -Method GET
            ## Are already in Color
            If ($colorEligibilityCheck.results.id.length -ge 1) {
                Write-Output "[INFO] $($ADUser) is marked eligible in Color"
                ## Check to see if they are already in the datahub
                $datahubCheckDataTable = New-Object System.Data.DataTable
                $sqlConnection = New-Object Data.SQLClient.SQLConnection "Server=$($vault.datahubURI);database=$($vault.datahubDatabase);trusted_connection=false;User ID=$($vault.datahubUsername);Password=$($vault.datahubPassword)"
                $sqlQuery = "SELECT [unique_identifier] FROM [BroadColor].[raw].[eligibility] WHERE [unique_identifier] like '$($ADUser)%'"
                $sqlConnection.open()
                $SQLCommand = $sqlConnection.CreateCommand()
                $sqlCommand.CommandText = $sqlQuery
                $sqlReader = $sqlCommand.ExecuteReader()
                $datahubCheckDataTable.Load($sqlReader)
                $sqlConnection.close()
                $datahubCheckDataTable = @($datahubCheckDataTable)
                If ($datahubCheckDataTable.length -ge 1){
                    Write-Output "[INFO] $($ADuser) entered into Datahub"
                }
                If ($datahubCheckDataTable.length -eq 0){
                    Write-Output "[WARNING] $($ADUser) does not exist in Datahub"
                }
            }
            ## Are not in Color
            If ($colorEligibilityCheck.results.id.length -eq 0){
                Write-Output "[INFO] $($ADUser) is not marked eligible in Color."
                $colorURI = "https://api.color.com/api/v1/external/eligibility/entries"
                $Payload = @{
                    unique_identifier = $($ADUser)
                    identifier_type = 'email'
                    population = 'Bay Path - Students'
                    external_id = $($ADUser)
                }
                $colorEligibilityAdd = Invoke-RestMethod -Headers $colorHeaders -Uri $colorUri -Method POST -Body $Payload
                Write-Output "[INFO] Re-checking eligibility"
                $colorURI = "https://api.color.com/api/v1/external/eligibility/entries?unique_identifiers=$($ADUser)"
                $colorEligibilityCheck = Invoke-RestMethod -Headers $colorHeaders -Uri $colorUri -Method GET
                If ($colorEligibilityCheck.results.id.length -ge 1) {
                    Write-Output "[INFO] $($ADUser) is marked eligible in Color. Color ID# is $($colorEligibilityCheck.results.id)"
                }
                ##Add to Datahub
                $sqlCommand = New-Object System.Data.SqlClient.SqlCommand
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@id",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@unique_identifier",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@population",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@external_id",[Data.SQLDBType]::nchar))) | Out-Null
                $SqlCommand.Parameters.Add((New-Object Data.SQLClient.SQLParameter("@created_at",[Data.SQLDBType]::datetime))) | Out-Null
                $SqlCommand.Parameters["@id"].Value = $colorEligibilityCheck.results.id
                $SqlCommand.Parameters["@unique_identifier"].Value = $colorEligibilityCheck.results.unique_identifier
                $SqlCommand.Parameters["@population"].Value = $colorEligibilityCheck.results.population
                $SqlCommand.Parameters["@external_id"].Value = $colorEligibilityCheck.results.external_id
                $SqlCommand.Parameters["@created_at"].Value = (Get-Date -Format "yyyy-MM-dd hh:mm:ss")
                $sqlQuery = "
                BEGIN TRAN
                    IF EXISTS (SELECT [id],[unique_identifier],[population],[external_id],[created_at] FROM [$($vault.datahubDatabase)].[raw].[eligibility] WHERE [id] = @id) 
                        UPDATE [$($vault.datahubDatabase)].[raw].[eligibility]
                            SET [unique_identifier] = @unique_identifier,
                                [population] = @population,
                                [external_id] = @external_id,
                                [created_at] = @created_at
                        WHERE [id] = @id;
                    ELSE
                        INSERT INTO [$($vault.datahubDatabase)].[raw].[eligibility] ([id],[unique_identifier],[population],[external_id],[created_at]) 
                        VALUES (@id,@unique_identifier,@population,@external_id,@created_at)
                COMMIT
                "
                $sqlConnection = New-Object Data.SQLClient.SQLConnection "Server=$($vault.datahubURI);database=$($vault.datahubDatabase);trusted_connection=false;User ID=$($vault.datahubUsername);Password=$($vault.datahubPassword)"
                $sqlConnection.open()
                $sqlCommand.CommandText = $sqlQuery
                $sqlCommand.Connection = $sqlConnection
                $sqlCMDStatus = $sqlCommand.ExecuteNonQuery()
                $sqlConnection.close()
            }
            
        }
}
Write-Output "[INFO] student records complete"
