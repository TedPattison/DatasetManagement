using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.PowerBI.Api;
using Microsoft.PowerBI.Api.Models;
using Microsoft.PowerBI.Api.Extensions;
using DatasetManagement.Models;
using System.IO;
using Microsoft.PowerBI.Api.Models.Credentials;
using System.Configuration;

namespace DatasetManagement.Models {
  class PowerBiManagerAppOnly {

    public static void GetAppWorkspaces() {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();

      var workspaces = pbiClient.Groups.GetGroups().Value;
      foreach (var workspace in workspaces) {
        Console.WriteLine(workspace.Name);
      }
    }

    public static Group GetAppWorkspace(string WorkspaceName) {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();

      var workspaces = pbiClient.Groups.GetGroups().Value;
      foreach (var workspace in workspaces) {
        if (workspace.Name.Equals(WorkspaceName))
          return workspace;
      }
      return null;
    }

    public static Dataset GetDataset(Guid WorkspaceId, string DatasetName) {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      var datasets = pbiClient.Datasets.GetDatasetsInGroup(WorkspaceId).Value;
      foreach (var dataset in datasets) {
        if (dataset.Name.Equals(DatasetName)) {
          return dataset;
        }
      }
      return null;
    }

    public static void GetDatasetInfo(string WorkspaceId, string DatasetId) {

      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      IList<Dataset> datasets = pbiClient.Datasets.GetDatasetsInGroup(new Guid(WorkspaceId)).Value;

      var dataset = datasets.Where(ds => ds.Id.Equals(DatasetId)).Single();

      Console.WriteLine(dataset.Name);

      IList<Datasource> datasources = pbiClient.Datasets.GetDatasourcesInGroup(new Guid(WorkspaceId), DatasetId).Value;

      foreach (var ds in datasources) {
        Console.WriteLine(ds.Name);
      }

      IList<Refresh> refreshes = null;
      if (dataset.IsRefreshable == true) {
        refreshes = pbiClient.Datasets.GetRefreshHistoryInGroup(new Guid(WorkspaceId), DatasetId).Value;
        foreach (var refresh in refreshes) {
          Console.WriteLine(refresh.RefreshType.Value + ": " + refresh.StartTime.Value.ToLocalTime());
        }
      }


    }

    public static void TakeOverDataset(Guid WorkspaceId, string DatasetId) {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      pbiClient.Datasets.TakeOverInGroup(WorkspaceId, DatasetId);

    }

    public static void PatchSqlDatasourceCredentials(Guid WorkspaceId, string DatasetId, string UserName, string UserPassword) {

      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      var datasources = (pbiClient.Datasets.GetDatasourcesInGroup(WorkspaceId, DatasetId)).Value;
      // find the target SQL datasource
      foreach (var datasource in datasources) {
        if (datasource.DatasourceType.ToLower() == "sql") {
          // get the datasourceId and the gatewayId
          var datasourceId = datasource.DatasourceId;
          var gatewayId = datasource.GatewayId;
          // Create UpdateDatasourceRequest to update Azure SQL datasource credentials
          UpdateDatasourceRequest req = new UpdateDatasourceRequest {
            CredentialDetails = new CredentialDetails(
              new BasicCredentials(UserName, UserPassword),
              PrivacyLevel.None,
              EncryptedConnection.NotEncrypted)
          };
          // Execute Patch command to update Azure SQL datasource credentials
          pbiClient.Gateways.UpdateDatasource((Guid)gatewayId, (Guid)datasourceId, req);
        }
      };

    }

    public static void PatchAnonymousDatasourceCredentials(Guid WorkspaceId, string DatasetId) {

      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      var datasources = pbiClient.Datasets.GetDatasourcesInGroup(WorkspaceId, DatasetId).Value;
      foreach (var datasource in datasources) {
        if (datasource.DatasourceType == "OAuth" || datasource.DatasourceType == "File") {
          var datasourceId = datasource.DatasourceId;
          var gatewayId = datasource.GatewayId;
          // create credentials for Azure SQL database log in
          CredentialDetails details = new CredentialDetails {
            CredentialType = CredentialType.Anonymous,
            PrivacyLevel = PrivacyLevel.None
          };
          UpdateDatasourceRequest req = new UpdateDatasourceRequest(details);
          // Update credentials through gateway
          pbiClient.Gateways.UpdateDatasourceAsync((Guid)gatewayId, (Guid)datasourceId, req);
        }
      }
      return;
    }

    public static void UpdateSqlDatabaseConnectionString(Guid WorkspaceId, string DatasetId, string Server, string Database) {

      var pbiClient = TokenManager.GetPowerBiAppOnlyClient();

      Datasource targetDatasource = pbiClient.Datasets.GetDatasourcesInGroup(WorkspaceId, DatasetId).Value.First();

      string currentServer = targetDatasource.ConnectionDetails.Server;
      string currentDatabase = targetDatasource.ConnectionDetails.Database;

      if (Server.ToLower().Equals(currentServer.ToLower()) && Database.ToLower().Equals(currentDatabase.ToLower())) {
        Console.WriteLine("New server and database name are the same as the old names");
        return;
      }

      DatasourceConnectionDetails connectionDetails = new DatasourceConnectionDetails {
        Database = Database,
        Server = Server
      };

      UpdateDatasourceConnectionRequest updateConnRequest =
        new UpdateDatasourceConnectionRequest {
          DatasourceSelector = targetDatasource,
          ConnectionDetails = connectionDetails
        };

      UpdateDatasourcesRequest updateDatasourcesRequest = new UpdateDatasourcesRequest(updateConnRequest);
      pbiClient.Datasets.UpdateDatasourcesInGroup(WorkspaceId, DatasetId, updateDatasourcesRequest);

    }

    public static void RefreshDataset(Guid WorkspaceId, string DatasetId) {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      pbiClient.Datasets.RefreshDatasetInGroup(WorkspaceId, DatasetId);
    }

    // testing operations

    public static Guid CreateAppWorkspace(string Name) {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      // create new app workspace
      GroupCreationRequest request = new GroupCreationRequest(Name);
      Group aws = pbiClient.Groups.CreateGroup(request);
      pbiClient.Groups.AddGroupUser(aws.Id,
        new GroupUser {
          EmailAddress = "tedp@mstdev2020.onmicrosoft.com",
          GroupUserAccessRight = GroupUserAccessRight.Admin
        });

      // return app workspace ID
      return aws.Id;
    }

    public static void PublishPBIX(Guid WorkspaceId, string PbixFilePath, string ImportName) {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      FileStream stream = new FileStream(PbixFilePath, FileMode.Open, FileAccess.Read);
      var import = pbiClient.Imports.PostImportWithFileInGroup(WorkspaceId, stream, ImportName);
    }

    public static void PublishPBIX(Guid WorkspaceId, byte[] Pbix, string ImportName) {
      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();
      MemoryStream stream = new MemoryStream(Properties.Resources.WingtipSales_pbix);
      var import = pbiClient.Imports.PostImportWithFileInGroup(WorkspaceId, stream, ImportName);
      Console.WriteLine("Publishing process completed");
    }

    public static void PublishContent(string WorkspaceName) {

      // create new workspace
      Guid workspaceId = CreateAppWorkspace(WorkspaceName);

      // import PBIX with SQL datasource
      string ImportName = "Wingtip Sales";
      PublishPBIX(workspaceId, Properties.Resources.WingtipSales_pbix, ImportName);
      // set datasource credentials
      var dataset = GetDataset(workspaceId, ImportName);
      PatchSqlDatasourceCredentials(workspaceId, dataset.Id, "CptStudent", "pass@word1");
      // refresh datasource
      RefreshDataset(workspaceId, dataset.Id);


      // import PBIX with anonymous access datasource
      string ImportName2 = "Northwind";
      PublishPBIX(workspaceId, Properties.Resources.NorthwindRetro_pbix, "Northwind");
      // set datasource credentials
      var dataset2 = GetDataset(workspaceId, ImportName2);
      PatchAnonymousDatasourceCredentials(workspaceId, dataset2.Id);
      // refresh datasource
      RefreshDataset(workspaceId, dataset2.Id);

    }

    public static void TakeOverDatasetAndRefresh(Guid WorkspaceId, string DatasetId, string UserName, string UserPassword) {

      PowerBIClient pbiClient = TokenManager.GetPowerBiAppOnlyClient();

      // take over dataset by service principal
      pbiClient.Datasets.TakeOverInGroup(WorkspaceId, DatasetId);

      // patch credentials for Sql datasource
      var datasources = (pbiClient.Datasets.GetDatasourcesInGroup(WorkspaceId, DatasetId)).Value;
      // find the target SQL datasource
      foreach (var datasource in datasources) {
        if (datasource.DatasourceType.ToLower() == "sql") {
          // get the datasourceId and the gatewayId
          var datasourceId = datasource.DatasourceId;
          var gatewayId = datasource.GatewayId;
          // Create UpdateDatasourceRequest to update Azure SQL datasource credentials
          UpdateDatasourceRequest req = new UpdateDatasourceRequest {
            CredentialDetails = new CredentialDetails(
              new BasicCredentials(UserName, UserPassword),
              PrivacyLevel.None,
              EncryptedConnection.NotEncrypted)
          };
          // Execute Patch command to update Azure SQL datasource credentials
          pbiClient.Gateways.UpdateDatasource((Guid)gatewayId, (Guid)datasourceId, req);
        }
      };

      // execute refresh command
      pbiClient.Datasets.RefreshDatasetInGroup(WorkspaceId, DatasetId);

    }


  }
}
