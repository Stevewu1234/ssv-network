import {ethers} from 'hardhat';
import * as fs from 'fs';

async function main() {
    const ssvRegistryFactory = await ethers.getContractFactory('SSVRegistry');
    const ssvRegistry = await ssvRegistryFactory.attach(
        process.env.SSV_REG_ADDR
    );

    const valsData = await fs.promises.readFile(process.env.SSV_VALIDATORS_JSON, 'utf-8');
    if (!valsData) {
        throw 'validators.json should be provided';
    }
    const vals = JSON.parse(valsData);
    for (const validator of vals) {
        await ssvRegistry.registerValidator(validator.address, validator.pk,
            validator.ops[0], validator.ops[1], validator.ops[2], validator.ops[3]);
    }
}

main()
    .then(() => process.exit(0))
    .catch(error => {
        console.error(error);
        process.exit(1);
    });
