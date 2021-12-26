import {ethers} from 'hardhat';
import * as fs from 'fs';

async function main() {
    const ssvRegistryFactory = await ethers.getContractFactory('SSVRegistry');
    const ssvRegistry = await ssvRegistryFactory.attach(
        process.env.SSV_REG_ADDR
    );

    const opsData = await fs.promises.readFile(process.env.SSV_OPERATORS_JSON, 'utf-8');
    if (!opsData) {
        throw 'operators.json should be provided';
    }
    const ops = JSON.parse(opsData);
    for (const op of ops) {
        await ssvRegistry.registerOperator(op.name, op.address, op.pk, op.fee || 1);
    }
}

main()
    .then(() => process.exit(0))
    .catch(error => {
        console.error(error);
        process.exit(1);
    });
