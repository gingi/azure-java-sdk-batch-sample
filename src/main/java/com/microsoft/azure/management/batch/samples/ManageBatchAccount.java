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

import com.microsoft.azure.PagedList;
import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.AllocationState;
import com.microsoft.azure.batch.protocol.models.CloudJob;
import com.microsoft.azure.batch.protocol.models.CloudPool;
import com.microsoft.azure.batch.protocol.models.CloudTask;
import com.microsoft.azure.batch.protocol.models.ImageReference;
import com.microsoft.azure.batch.protocol.models.PoolAddParameter;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import com.microsoft.azure.batch.protocol.models.TaskAddParameter;
import com.microsoft.azure.batch.protocol.models.TaskState;
import com.microsoft.azure.batch.protocol.models.VirtualMachineConfiguration;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.batch.BatchAccount;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.microsoft.azure.management.samples.Utils;
import com.microsoft.rest.LogLevel;

public final class ManageBatchAccount {
    static final long POOL_STEADY_TIMEOUT_IN_SECONDS = 300;
    static final long TASK_STATUS_TIMEOUT_IN_SECONDS = 10;
    static final long POOL_STATUS_TIMEOUT_IN_SECONDS = 20;
    static final long NUM_TASK_STATUS_CHECKS = 300;
    static final long TASK_CHUNK_SIZE = 100;
    static final long NUM_TASKS = 1200;

    /**
     * Main function which runs the actual sample.
     * @param azure instance of the azure client
     * @return true if sample runs successfully
     */
    public static boolean runSample(Azure azure) {
        final String batchAccountName = "shpasterbatch1";
        final String storageAccountName = Utils.createRandomName("sa");
        final String applicationName = "application";
        final String applicationDisplayName = "My application display name";
        final String applicationPackageName = "app_package";
        final String rgName = "sdktest";
        final Region region = Region.US_EAST;
        final String poolId = "javaSdkTest";
        final String jobId = Utils.createRandomName("sdktest");

        BatchClient client = null;

        try {

            // ===========================================================
            // Get how many batch accounts can be created in specified region.

            int allowedNumberOfBatchAccounts = azure.batchAccounts().getBatchAccountQuotaByLocation(region);

            // ===========================================================
            // List all the batch accounts in subscription.

            List<BatchAccount> batchAccounts = azure.batchAccounts().list();
            int batchAccountsAtSpecificRegion = 0;
            BatchAccount batchAccount = null;
            for (BatchAccount account: batchAccounts) {
                if (account.region() == region) {
                    batchAccountsAtSpecificRegion++;
                }
                if (batchAccountName.equals(account.name())) {
                    batchAccount = account;
                }
            }

            if (batchAccount == null && batchAccountsAtSpecificRegion >= allowedNumberOfBatchAccounts) {
                System.out.println("No more batch accounts can be created at "
                        + region + " region, this region already have "
                        + batchAccountsAtSpecificRegion
                        + " batch accounts, current quota to create batch account in "
                        + region + " region is " +  allowedNumberOfBatchAccounts + ".");
                return false;
            }

            // ============================================================
            // Create a batch account

            if (batchAccount != null) {
                System.out.println("Found account " + batchAccountName);
            } else {
                System.out.println("Creating a batch Account");

                batchAccount = azure.batchAccounts().define(batchAccountName)
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
            }

            client = getBatchClient(batchAccount);
            System.out.println("Created client with " + batchAccount.accountEndpoint());

            // CREATE JOB
            PoolInformation poolInfo = new PoolInformation().withPoolId(poolId);
            
            if (!client.poolOperations().existsPool(poolId)) {
                System.out.println("Creating pool " + poolId);

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
            }

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
                if (taskList.size() >= TASK_CHUNK_SIZE || i == NUM_TASKS - 1) {
                    System.out.println("Submitting " + taskList.size() + " tasks");
                    client.taskOperations().createTasks(jobId, taskList);
                    taskList.clear();
                }
            }

            System.out.println("Created " + NUM_TASKS + " tasks");
            for (int i = 0; i < NUM_TASK_STATUS_CHECKS; i++) {
                Thread.sleep(TASK_STATUS_TIMEOUT_IN_SECONDS * 1000);

                /***** The problematic call: *****/
                PagedList<CloudTask> tasks = client.taskOperations().listTasks(jobId);

                System.out.println("Tasks remaining:" + tasks.size() + " hasNexPage:" + tasks.hasNextPage() + " ");
                System.out.println("  NextPageLink: " + tasks.currentPage().nextPageLink());

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
            if (client != null) {
                try {
                    System.out.println("Deleting job " + jobId);
                    client.jobOperations().deleteJob(jobId);
                } catch (Exception e) {
                    System.err.println("Could not delete job " + jobId);
                    e.printStackTrace();
                }
            } else {
                System.out.println("no client");
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
                    .withLogLevel(LogLevel.BODY_AND_HEADERS)
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
                System.out.println("[" + pool.allocationState() + "] wait 10 seconds for pool steady...");
                Thread.sleep(POOL_STATUS_TIMEOUT_IN_SECONDS * 1000);
                elapsedTime = (new Date()).getTime() - startTime;
            }
        } catch (Exception e) {
            System.err.println("Pool not reached steady state properly");
            e.printStackTrace();
        }
        System.out.println("Pool reached steady? " + steady);
    }
}