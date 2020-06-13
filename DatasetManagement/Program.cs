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

namespace DatasetManagement {

  class Program {

    static void Main() {

     CreateAndPopulateWorkspaceAsUser();
     
     //  CreateAndPopulateWorkspaceAsServicePrincipal();
     //  TakeOverDatasetAndRefreshTest();
    
    }

    static void CreateAndPopulateWorkspaceAsUser() {
      PowerBiManager.PublishContent("Workspace 1");
    }

    static void CreateAndPopulateWorkspaceAsServicePrincipal() {
      PowerBiManagerAppOnly.PublishContent("Workspace 2");
    }

    static void TakeOverDatasetAndRefreshTest() {
      Group workspace = PowerBiManagerAppOnly.GetAppWorkspace("Workspace 1");
      Dataset dataset = PowerBiManagerAppOnly.GetDataset(workspace.Id, "Wingtip Sales");
      PowerBiManagerAppOnly.TakeOverDatasetAndRefresh(workspace.Id, dataset.Id, "CptStudent", "pass@word1");

    }



  }
}
