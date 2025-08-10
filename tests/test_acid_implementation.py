#!/usr/bin/env python
"""
Test script for ACID compliance tests with real database operations
"""

import logging
import sys
from typing import Any, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Add the parent directory to the path so we can import modules
sys.path.append(".")

from database.connector_factory import DatabaseConnectorFactory
from dotenv import load_dotenv

# Import required modules
from tests.acid_tests import ACIDTests


def test_consistency(provider: str = "google_spanner") -> Dict[str, Any]:
    """Test the consistency implementation"""
    try:
        print(f"🧪 Testing Consistency with {provider}...")
        connector = DatabaseConnectorFactory.create_connector(provider)

        # Use ACID test implementation
        acid_tests = ACIDTests(connector)

        print(f"✅ ACID tests initialized for {acid_tests.provider_name}")
        print(f"✅ Test session ID: {acid_tests.test_id}")

        # Run consistency test
        print("🔄 Running Consistency Test...")
        result = acid_tests.test_consistency()

        print("✅ Consistency Test Result:")
        print(f"   Status: {result['status']}")
        print(f"   Provider: {result.get('provider', 'Unknown')}")
        print(f"   Description: {result.get('description', 'N/A')}")
        print(f"   Duration: {result.get('duration', 'N/A')}")
        if 'error' in result:
            print(f"   Error: {result['error']}")

        if "constraint_tests" in result:
            print(f"   Constraint Tests: {len(result['constraint_tests'])} tests")
            for test in result["constraint_tests"]:
                print(
                    f"     - {test['test']}: {'✅ PASSED' if test['passed'] else '❌ FAILED'}"
                )

        print("🎉 Consistency test completed successfully!")
        return result

    except Exception as e:
        logger.error(f"❌ Consistency test error: {str(e)}")
        import traceback

        traceback.print_exc()
        return {"status": "failed", "error": str(e)}


def test_atomicity(provider: str = "google_spanner") -> Dict[str, Any]:
    """Test the atomicity implementation"""
    try:
        print(f"🧪 Testing Atomicity with {provider}...")
        connector = DatabaseConnectorFactory.create_connector(provider)

        # Use ACID test implementation
        acid_tests = ACIDTests(connector)

        # Run atomicity test
        print("🔄 Running Atomicity Test...")
        result = acid_tests.test_atomicity()

        print("✅ Atomicity Test Result:")
        print(f"   Status: {result['status']}")
        print(f"   Provider: {result.get('provider', 'Unknown')}")
        print(f"   Description: {result.get('description', 'N/A')}")
        print(f"   Atomicity Verified: {result.get('atomicity_verified', False)}")
        print(f"   Duration: {result.get('duration', 'N/A')}")
        if 'error' in result:
            print(f"   Error: {result['error']}")

        print("🎉 Atomicity test completed successfully!")
        return result

    except Exception as e:
        logger.error(f"❌ Atomicity test error: {str(e)}")
        import traceback

        traceback.print_exc()
        return {"status": "failed", "error": str(e)}


def test_isolation(provider: str = "google_spanner") -> Dict[str, Any]:
    """Test the isolation implementation"""
    try:
        print(f"🧪 Testing Isolation with {provider}...")
        connector = DatabaseConnectorFactory.create_connector(provider)

        # Use ACID test implementation
        acid_tests = ACIDTests(connector)

        # Run isolation test
        print("🔄 Running Isolation Test...")
        result = acid_tests.test_isolation()

        print("✅ Isolation Test Result:")
        print(f"   Status: {result['status']}")
        print(f"   Provider: {result.get('provider', 'Unknown')}")
        print(f"   Description: {result.get('description', 'N/A')}")
        print(f"   Duration: {result.get('duration', 'N/A')}")
        if 'error' in result:
            print(f"   Error: {result['error']}")

        if "isolation_tests" in result:
            print(f"   Isolation Tests: {len(result['isolation_tests'])} tests")
            for test in result["isolation_tests"]:
                print(
                    f"     - {test['test']}: {'✅ PASSED' if test['passed'] else '❌ FAILED'}"
                )

        print("🎉 Isolation test completed successfully!")
        return result

    except Exception as e:
        logger.error(f"❌ Isolation test error: {str(e)}")
        import traceback

        traceback.print_exc()
        return {"status": "failed", "error": str(e)}


def test_durability(provider: str = "google_spanner") -> Dict[str, Any]:
    """Test the durability implementation"""
    try:
        print(f"🧪 Testing Durability with {provider}...")
        connector = DatabaseConnectorFactory.create_connector(provider)

        # Use ACID test implementation
        acid_tests = ACIDTests(connector)

        # Run durability test
        print("🔄 Running Durability Test...")
        result = acid_tests.test_durability()

        print("✅ Durability Test Result:")
        print(f"   Status: {result['status']}")
        print(f"   Provider: {result.get('provider', 'Unknown')}")
        print(f"   Description: {result.get('description', 'N/A')}")
        print(f"   Data Persisted: {result.get('details', '')}")
        print(f"   Duration: {result.get('duration', 'N/A')}")
        if 'error' in result:
            print(f"   Error: {result['error']}")

        print("🎉 Durability test completed successfully!")
        return result

    except Exception as e:
        logger.error(f"❌ Durability test error: {str(e)}")
        import traceback

        traceback.print_exc()
        return {"status": "failed", "error": str(e)}


def test_all(provider: str = "google_spanner") -> Dict[str, Any]:
    """Run all ACID tests"""
    try:
        print(f"🧪 Running all ACID tests with {provider}...")
        connector = DatabaseConnectorFactory.create_connector(provider)

        # Use ACID test implementation
        acid_tests = ACIDTests(connector)
        result = acid_tests.run_all_tests()

        print("✅ All ACID Tests Result:")
        print(f"   Provider: {result['provider']}")
        print(f"   Test Suite: {result['test_suite']}")
        print(f"   Total Tests: {result['summary']['total_tests']}")
        print(f"   Passed Tests: {result['summary']['passed_tests']}")
        print(f"   Failed Tests: {result['summary']['failed_tests']}")
        print(f"   Success Rate: {result['summary']['success_rate']:.1f}%")
        print(f"   Total Duration: {result['summary'].get('duration', 'N/A')}")

        # Print individual test results with durations
        for test_name, test_result in result["tests"].items():
            print(
                f"   {test_name.capitalize()}: {test_result['status'].upper()} (Duration: {test_result.get('duration', 'N/A')})"
            )

        print("🎉 All ACID tests completed!")
        return result

    except Exception as e:
        logger.error(f"❌ All ACID tests error: {str(e)}")
        import traceback

        traceback.print_exc()
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Process command line arguments
    import argparse

    parser = argparse.ArgumentParser(
        description="Test ACID compliance with real database operations"
    )
    parser.add_argument(
        "--test",
        choices=["atomicity", "consistency", "isolation", "durability", "all"],
        default="all",
        help="Which ACID test to run (default: all)",
    )
    parser.add_argument(
        "--provider",
        default="google_spanner",
        help="Database provider to test (default: google_spanner)",
    )
    args = parser.parse_args()

    # Run the selected test with the specified provider
    if args.test == "atomicity":
        test_atomicity(args.provider)
    elif args.test == "consistency":
        test_consistency(args.provider)
    elif args.test == "isolation":
        test_isolation(args.provider)
    elif args.test == "durability":
        test_durability(args.provider)
    else:
        test_all(args.provider)
