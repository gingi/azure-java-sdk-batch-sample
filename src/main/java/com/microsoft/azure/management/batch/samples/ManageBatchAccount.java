/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.azure.management.batch.samples;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import com.microsoft.azure.Page;
import com.microsoft.azure.PagedList;
import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.implementation.TasksImpl;
import com.microsoft.azure.batch.protocol.models.AllocationState;
import com.microsoft.azure.batch.protocol.models.CloudJob;
import com.microsoft.azure.batch.protocol.models.CloudPool;
import com.microsoft.azure.batch.protocol.models.CloudTask;
import com.microsoft.azure.batch.protocol.models.ImageReference;
import com.microsoft.azure.batch.protocol.models.PoolAddParameter;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import com.microsoft.azure.batch.protocol.models.TaskAddParameter;
import com.microsoft.azure.batch.protocol.models.TaskListHeaders;
import com.microsoft.azure.batch.protocol.models.TaskState;
import com.microsoft.azure.batch.protocol.models.VirtualMachineConfiguration;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.batch.BatchAccount;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.microsoft.azure.management.samples.Utils;
import com.microsoft.rest.LogLevel;
import com.microsoft.rest.ServiceResponseWithHeaders;

/**
 * Azure Batch sample for managing batch accounts -
 *  - Get subscription batch account quota for a particular location.
 *  - List all the batch accounts, look if quota allows you to create a new batch account at specified location by counting batch accounts in that particular location.
 *  - Create a batch account with new application and application package, along with new storage account.
 *  - Get the keys for batch account.
 *  - Regenerate keys for batch account
 *  - Regenerate the keys of storage accounts, sync with batch account.
 *  - Update application's display name.
 *  - Create another batch account using existing storage account.
 *  - List the batch account.
 *  - Delete the batch account.
 *      - Delete the application packages.
 *      - Delete applications.
 */

public final class ManageBatchAccount {
    static final long POOL_STEADY_TIMEOUT_IN_SECONDS = 300;
    static final long NUM_TASKS = 1500;

    /**
     * Main function which runs the actual sample.
     * @param azure instance of the azure client
     * @return true if sample runs successfully
     */
    public static boolean runSample(Azure azure) {
        final String batchAccountName = Utils.createRandomName("ba");
        final String storageAccountName = Utils.createRandomName("sa");
        final String applicationName = "application";
        final String applicationDisplayName = "My application display name";
        final String applicationPackageName = "app_package";
        final String rgName = Utils.createRandomName("testPagedList");
        final Region region = Region.US_EAST;
        final String poolId = Utils.createRandomName("shpasterPool");
        final String jobId = Utils.createRandomName("shpasterJob");

        try {

            // ===========================================================
            // Get how many batch accounts can be created in specified region.

            int allowedNumberOfBatchAccounts = azure.batchAccounts().getBatchAccountQuotaByLocation(region);

            // ===========================================================
            // List all the batch accounts in subscription.

            List<BatchAccount> batchAccounts = azure.batchAccounts().list();
            int batchAccountsAtSpecificRegion = 0;
            for (BatchAccount batchAccount: batchAccounts) {
                if (batchAccount.region() == region) {
                    batchAccountsAtSpecificRegion++;
                }
            }

            if (batchAccountsAtSpecificRegion >= allowedNumberOfBatchAccounts) {
                System.out.println("No more batch accounts can be created at "
                        + region + " region, this region already have "
                        + batchAccountsAtSpecificRegion
                        + " batch accounts, current quota to create batch account in "
                        + region + " region is " +  allowedNumberOfBatchAccounts + ".");
                return false;
            }

            // ============================================================
            // Create a batch account

            System.out.println("Creating a batch Account");

            BatchAccount batchAccount = azure.batchAccounts().define(batchAccountName)
                    .withRegion(region)
                    .withNewResourceGroup(rgName)
                    .defineNewApplication(applicationName)
                        .defineNewApplicationPackage(applicationPackageName)
                        .withAllowUpdates(true)
                        .withDisplayName(applicationDisplayName)
                        .attach()
                    .withNewStorageAccount(storageAccountName)
                    .create();

            System.out.println("Created a batch Account:");
            Utils.print(batchAccount);

            // ============================================================
            // Update name of application.
            batchAccount
                    .update()
                    .updateApplication(applicationName)
                    .withDisplayName("New application display name")
                    .parent()
                    .apply();

            batchAccount.refresh();
            Utils.print(batchAccount);

            // ============================================================
            // List batch accounts

            System.out.println("Listing Batch accounts");

            List<BatchAccount> accounts = azure.batchAccounts().listByResourceGroup(rgName);
            BatchAccount ba;
            for (int i = 0; i < accounts.size(); i++) {
                ba = accounts.get(i);
                System.out.println("Batch Account (" + i + ") " + ba.name());
            }

            // ============================================================
            // Refresh a batch account.
            batchAccount.refresh();
            Utils.print(batchAccount);

            BatchClient client = getBatchClient(batchAccount);
            System.out.println("Created client with " + batchAccount.accountEndpoint());

            // CREATE JOB

            System.out.println("Creating pool " + poolId);
            PoolInformation poolInfo = new PoolInformation().withPoolId(poolId);

            ImageReference imgRef = new ImageReference()
                .withPublisher("Canonical").withOffer("UbuntuServer")
                .withSku("16.04-LTS").withVersion("latest");
            VirtualMachineConfiguration poolConfiguration =
                new VirtualMachineConfiguration();
            poolConfiguration
                .withNodeAgentSKUId("batch.node.ubuntu 16.04")
                .withImageReference(imgRef);

            PoolAddParameter poolParam = new PoolAddParameter()
                .withId(poolId)
                .withVmSize("STANDARD_D1_V2")
                .withTargetDedicatedNodes(3)
                .withVirtualMachineConfiguration(poolConfiguration);
            client.poolOperations().createPool(poolParam);

            waitForPool(client, poolId);
            System.out.println("Pool created");

            System.out.println("Creating job " + jobId);
            client.jobOperations().createJob(jobId, poolInfo);

            for (CloudJob job: client.jobOperations().listJobs()) {
                System.out.println("job " + job.url());
            }
            List<TaskAddParameter> taskList = new ArrayList<TaskAddParameter>();

            for (int i = 0; i < NUM_TASKS; i++) {
                TaskAddParameter param = new TaskAddParameter()
                    .withId(jobId + "-" + i)
                    .withCommandLine("sleep 30");
                taskList.add(param);
            }

            System.out.println("Creating " + taskList.size() + " tasks");
            client.taskOperations().createTasks(jobId, taskList);

            System.out.println("Created tasks");
            for (int i = 0; i < 300; i++) {
                Thread.sleep(10 * 1000);
                PagedList<CloudTask> tasks = client.taskOperations().listTasks(jobId, new DetailLevel.Builder().withSelectClause("state").build());
                System.out.println("Tasks remaining:" + tasks.size() + " hasNexPage:" + tasks.hasNextPage());
                System.out.println("  NextPageLink: " + tasks.currentPage().nextPageLink());

                TasksImpl impl = (TasksImpl) client.protocolLayer().tasks();
                ServiceResponseWithHeaders<Page<CloudTask>, TaskListHeaders> response =
                    impl.listSinglePageAsync(jobId).toBlocking().single();

                HashMap<TaskState, Integer> map = new LinkedHashMap<>();
                for (CloudTask task: tasks) {
                    TaskState state =  task.state();
                    Integer count =  map.get(state);
                    if (count == null) {
                        count = 0;
                    }
                    map.put(state, count+1);
                }
                for (TaskState state: map.keySet()) {
                    System.out.println("  " + state.name() + ": " + map.get(state));
                }
            }

            return true;
        } catch (Exception f) {
            System.out.println(f.getMessage());
            f.printStackTrace();
        } finally {
            try {
                System.out.println("Deleting Resource Group: " + rgName);
                azure.resourceGroups().deleteByName(rgName);
                System.out.println("Deleted Resource Group: " + rgName);
            }
            catch (Exception e) {
                System.out.println("Did not create any resources in Azure. No clean up is necessary");
            }
        }
        return false;
    }

    /**
     * Main entry point.
     * @param args the parameters
     */
    public static void main(String[] args) {

        try {

            final File credFile = new File(System.getenv("AZURE_AUTH_LOCATION"));

            Azure azure = Azure.configure()
                    .withLogLevel(LogLevel.BASIC)
                    .authenticate(credFile)
                    .withDefaultSubscription();

            // Print selected subscription
            System.out.println("Selected subscription: " + azure.subscriptionId());

            runSample(azure);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private ManageBatchAccount() {
    }

    private static BatchClient getBatchClient(BatchAccount batchAccount) {
        BatchSharedKeyCredentials creds = new BatchSharedKeyCredentials(
            "https://" + batchAccount.accountEndpoint(),
            batchAccount.name(),
            batchAccount.getKeys().primary()
        );
        return BatchClient.open(creds);
    }

    private static void waitForPool(BatchClient client, String poolId) {
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;
        boolean steady = false;
        CloudPool pool;

        try {

            // Wait for the VM to be allocated
            while (elapsedTime < POOL_STEADY_TIMEOUT_IN_SECONDS * 1000) {
                pool = client.poolOperations().getPool(poolId);
                if (pool.allocationState() == AllocationState.STEADY) {
                    steady = true;
                    break;
                }
                System.out.println("wait 30 seconds for pool steady...");
                Thread.sleep(30 * 1000);
                elapsedTime = (new Date()).getTime() - startTime;
            }
        } catch (Exception e) {
            System.err.println("Pool not reached steady state properly");
            e.printStackTrace();
        }
        System.out.println("Pool reached steady? " + steady);
    }
}