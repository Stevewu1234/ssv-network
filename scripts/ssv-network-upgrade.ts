import { ethers, upgrades } from 'hardhat';
const fs = require('fs');
const path = require('path');
const hre = require('hardhat');
const ghpages = require('gh-pages');

async function main() {
  const fullNames = await hre.artifacts.getAllFullyQualifiedNames();
  const ssvNetworkName = fullNames.find(name => name.includes('/SSVNetwork.sol'));
  const { abi, contractName } = await hre.artifacts.readArtifact(ssvNetworkName);
  const dir = path.dirname(hre.config.paths.root);
  console.log(dir);
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.writeFile(`${dir}/${contractName.toLowerCase()}.json`, `${JSON.stringify(abi, null, 2)}\n`, { flag: 'w' });
  /*
  await ghpages.publish('abi', (err) => {
    console.log(err, '333');
  });
  */
  // await fs.promises.rmSync(dir, { recursive: true, force: true });
  /*
  const proxyAddress = process.env.PROXY_ADDRESS;
  const ContractUpgraded = await ethers.getContractFactory('SSVNetwork');
  console.log('Running upgrade...');
  const newContract = await upgrades.upgradeProxy(proxyAddress, ContractUpgraded);
  console.log(`SSVNetwork upgraded at: ${newContract.address}`);
  */
}

main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error);
    process.exit(1);
  });
