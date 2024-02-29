// Import ethers from Hardhat package
const { ethers } = require("hardhat");

async function main() {
  // This will compile your contracts if they haven't been compiled already
  const MLJobs = await ethers.getContractFactory("MLJobs");
  
  // Deploy the contract
  const mlJobs = await MLJobs.deploy();
  await mlJobs.deployed();

  console.log("MLJobs deployed to:", mlJobs.address);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
