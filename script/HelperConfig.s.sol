// SPDX-License-Identifier: MIT


// 1. Deploy mocks when we are on a local anvil chain
// 2. Keep track of contract address across different chains
// Sepolia ETH/USD
// Mainnet ETH/USD
pragma solidity ^0.8.19;

import {Script} from "forge-std/Script.sol";
import {MockV3Aggregator} from "../test/mocks/MockV3Aggregator.sol";


contract HelperConfig is Script {
    // If we are on a local anvil chain, deploy mocks
    // Otherwise, grab the existing address from the

    NetworkConfig public activeNetworkConfig; 

    uint8 public constant DECIMALS = 8; 
    int256 public constant INITIAL_ANSWER = 2000e8;

    struct NetworkConfig {
        address priceFeed;
    }

    constructor() {
        if (block.chainid == 11155111) {
            activeNetworkConfig = getSepoliaEthConfig();
        } else if (block.chainid == 1) {
            // mainnet
            activeNetworkConfig = getSepoliaEthConfig();
        }
        else {
            activeNetworkConfig = getOrCreateAnvilEthConfig();
        }
    }

    function getSepoliaEthConfig() public pure returns (NetworkConfig memory) {
        // price feed address
        NetworkConfig memory sepoliaConfig = NetworkConfig({ 
            priceFeed: 0x694AA1769357215DE4FAC081bf1f309aDC325306
        });
        return sepoliaConfig;
    } 
    
    function getOrCreateAnvilEthConfig() public  returns (NetworkConfig memory) {
        if (activeNetworkConfig.priceFeed != address(0)) {
            return activeNetworkConfig;
        }
        // price feed address

        //1 deploy the mocks
        //2 return the address of the mock

        vm.startBroadcast();
        MockV3Aggregator mockPriceFeed = new MockV3Aggregator(DECIMALS, INITIAL_ANSWER);
        vm.stopBroadcast();

        NetworkConfig memory anvilConfig = NetworkConfig({
            priceFeed: address(mockPriceFeed)
        });
        return anvilConfig;


    }
}