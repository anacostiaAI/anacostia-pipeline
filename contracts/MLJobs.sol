// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract MLJobs {
    struct Job {
        address owner;
        string modelDetails;
        bool completed;
    }

    Job[] public jobs;

    event JobCreated(uint jobId, address owner, string modelDetails);
    event JobCompleted(uint jobId, bool completed);

    function createJob(string memory modelDetails) public {
        jobs.push(Job(msg.sender, modelDetails, false));
        emit JobCreated(jobs.length - 1, msg.sender, modelDetails);
    }

    function completeJob(uint jobId) public {
        Job storage job = jobs[jobId];
        require(msg.sender == job.owner, "Only the job owner can complete the job.");
        job.completed = true;
        emit JobCompleted(jobId, true);
    }

    function getJob(uint jobId) public view returns (address owner, string memory modelDetails, bool completed) {
        Job storage job = jobs[jobId];
        return (job.owner, job.modelDetails, job.completed);
    }
}
