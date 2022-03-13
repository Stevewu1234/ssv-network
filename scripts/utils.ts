const fs = require('fs');
const hre = require('hardhat');
const ghpages = require('gh-pages');

export async function publishAbi() {
  const fullNames = await hre.artifacts.getAllFullyQualifiedNames();
  const ssvNetworkName = fullNames.find(name => name.includes('/SSVNetwork.sol'));
  const { abi, contractName } = await hre.artifacts.readArtifact(ssvNetworkName);
  const dir = `${hre.config.paths.root}/dist`;
  await fs.promises.mkdir(`${dir}/abi`, { recursive: true });
  await fs.promises.writeFile(`${dir}/abi/${contractName.toLowerCase()}.json`, `${JSON.stringify(abi, null, 2)}\n`, { flag: 'w' });
  await ghpages.publish(dir, (err) => console.log);
}