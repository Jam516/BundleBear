## BundleBear 🐻✨

BundleBear is a dataset that tracks the adoption of smart accounts.

The SQL queries in this [dbt](https://docs.getdbt.com/docs/introduction) project run once a day to transform raw and decoded blockchain data into easy-to-interpret records of every smart account transaction.

### How to contribute to the Operator Registry:

The Operator Registry stores labels for the contracts and EOAs of ERC-4337 bundlers, paymasters, factories and apps. To add a label you have to make a pull request to this repo. Once your label is in the dataset, the BundleBear team will update the BundleBear [dashboard](https://www.bundlebear.com/overview/all) so that your app or infrastructure provider is properly represented there.

1. Fork the BundleBear repo (button towards the top right of the repo page).

2. Start a new branch in your forked repo.

3. Edit the appropriate label query in the BundleBear/models/erc4337/labels folder. The queries are as follows

    - BundleBear/models/erc4337/labels/erc4337_labels_apps.sql = Edit this to label a new app contract
    - BundleBear/models/erc4337/labels/erc4337_labels_bundlers.sql = Edit this to label a new bundler EOA
    - BundleBear/models/erc4337/labels/erc4337_labels_factories.sql = Edit this to label a new factory contract
    - BundleBear/models/erc4337/labels/erc4337_labels_paymasters.sql = Edit this to label a new paymaster contract

    Note: All addresses need to be in lowercase!

4. Make a Pull Request on your new branch, to the main BundleBear repo, with a brief explanation of what you changed.

5. Wait for someone to either comment on or merge your Pull Request. 

6. Once your PR has been merged, please give 24 hours for the front-end team to make any needed changes on the BundleBear dashboard site.

### Resources:
- Read the [4337 proposal](https://eips.ethereum.org/EIPS/eip-4337)
- Learn more about [4337 data analysis](https://docs.getdbt.com/docs/introduction)
