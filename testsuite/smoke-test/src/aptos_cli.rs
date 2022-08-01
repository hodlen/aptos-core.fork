// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::smoke_test_environment::new_local_swarm_with_aptos;
use aptos::{account::create::DEFAULT_FUNDED_COINS, test::CliTestFramework};
use aptos_config::{keys::ConfigKey, utils::get_available_port};
use aptos_crypto::ed25519::Ed25519PrivateKey;
use aptos_crypto::{bls12381, ValidCryptoMaterialStringExt};
use aptos_faucet::FaucetArgs;
use aptos_genesis::config::HostAndPort;
use aptos_keygen::KeyGen;
use aptos_types::{
    account_config::aptos_root_address, chain_id::ChainId, network_address::DnsName,
};
use forge::{LocalSwarm, Node};
use std::convert::TryFrom;
use std::path::PathBuf;
use tokio::task::JoinHandle;

pub async fn setup_cli_test(
    num_nodes: usize,
    num_cli_accounts: usize,
) -> (LocalSwarm, CliTestFramework, JoinHandle<()>) {
    let swarm = new_local_swarm_with_aptos(num_nodes).await;
    let chain_id = swarm.chain_id();
    let validator = swarm.validators().next().unwrap();
    let root_key = swarm.root_key();
    let faucet_port = get_available_port();
    let faucet = launch_faucet(
        validator.rest_api_endpoint(),
        root_key,
        chain_id,
        faucet_port,
    );
    let faucet_endpoint: reqwest::Url =
        format!("http://localhost:{}", faucet_port).parse().unwrap();
    // Connect the operator tool to the node's JSON RPC API
    let tool = CliTestFramework::new(
        validator.rest_api_endpoint(),
        faucet_endpoint,
        num_cli_accounts,
    )
    .await;

    (swarm, tool, faucet)
}

pub fn launch_faucet(
    endpoint: reqwest::Url,
    mint_key: Ed25519PrivateKey,
    chain_id: ChainId,
    port: u16,
) -> JoinHandle<()> {
    let faucet = FaucetArgs {
        address: "127.0.0.1".to_string(),
        port,
        server_url: endpoint,
        mint_key_file_path: PathBuf::new(),
        mint_key: Some(ConfigKey::new(mint_key)),
        mint_account_address: Some(aptos_root_address()),
        chain_id,
        maximum_amount: None,
        do_not_delegate: true,
    };
    tokio::spawn(faucet.run())
}

#[tokio::test]
async fn test_account_flow() {
    let (_swarm, cli, _faucet) = setup_cli_test(1, 2).await;

    assert_eq!(DEFAULT_FUNDED_COINS, cli.account_balance(0).await.unwrap());
    assert_eq!(DEFAULT_FUNDED_COINS, cli.account_balance(1).await.unwrap());

    // Transfer an amount between the accounts
    let transfer_amount = 100;
    let response = cli.transfer_coins(0, 1, transfer_amount).await.unwrap();
    let expected_sender_amount =
        DEFAULT_FUNDED_COINS - response.gas_used.unwrap() - transfer_amount;
    let expected_receiver_amount = DEFAULT_FUNDED_COINS + transfer_amount;

    assert_eq!(
        expected_sender_amount,
        cli.wait_for_balance(0, expected_sender_amount)
            .await
            .unwrap()
    );
    assert_eq!(
        expected_receiver_amount,
        cli.wait_for_balance(1, expected_receiver_amount)
            .await
            .unwrap()
    );

    // Wait for faucet amount to be updated
    let expected_sender_amount = expected_sender_amount + DEFAULT_FUNDED_COINS;
    let _ = cli.fund_account(0).await.unwrap();
    assert_eq!(
        expected_sender_amount,
        cli.wait_for_balance(0, expected_sender_amount)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_register_and_update_validator() {
    let (_swarm, cli, _faucet) = setup_cli_test(1, 0).await;
    let mut keygen = KeyGen::from_os_rng();
    let private_key = keygen.generate_ed25519_private_key();
    let network_private_key = keygen.generate_x25519_private_key().unwrap();
    let consensus_private_key = keygen.generate_bls12381_private_key();
    let consensus_public_key = bls12381::PublicKey::from(&consensus_private_key);
    let validator_cli_index = 0;
    cli.init(validator_cli_index, &private_key).await.unwrap();
    assert_eq!(
        DEFAULT_FUNDED_COINS,
        cli.account_balance(validator_cli_index).await.unwrap()
    );

    let validator_config = cli.show_validator_config(validator_cli_index).await;
    assert!(validator_config.is_err()); // validator not registered

    let port = 1234;
    cli.register_validator_candidate(
        validator_cli_index,
        bls12381::PublicKey::from(&consensus_private_key),
        bls12381::ProofOfPossession::create(&consensus_private_key),
        HostAndPort {
            host: DnsName::try_from("0.0.0.0".to_string()).unwrap(),
            port,
        },
        network_private_key.public_key(),
    )
    .await
    .unwrap();

    let validator_config = cli
        .show_validator_config(validator_cli_index)
        .await
        .unwrap();
    assert_eq!(
        bls12381::PublicKey::from_encoded_string(
            validator_config
                .as_object()
                .unwrap()
                .get("consensus_pubkey")
                .unwrap()
                .as_str()
                .unwrap()
        )
        .unwrap(),
        consensus_public_key
    );
}
