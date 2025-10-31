#!/bin/bash
# Test runner for NoiseModelling with different database backends

set -e

echo "═══════════════════════════════════════════════════════════════"
echo "  NoiseModelling Test Runner"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

cd wps_scripts

# Function to run tests
run_test() {
    local mode=$1
    local description=$2
    local command=$3
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Testing: ${description}${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if eval "$command"; then
        echo ""
        echo -e "${GREEN}✅ ${mode} tests PASSED${NC}"
        return 0
    else
        echo ""
        echo -e "${RED}❌ ${mode} tests FAILED${NC}"
        return 1
    fi
}

# Parse command line arguments
MODE="${1:-all}"

case "$MODE" in
    "h2gis")
        echo "Running H2GIS tests only..."
        run_test "H2GIS" "In-memory H2GIS database" \
            "./gradlew test"
        ;;
        
    "postgis")
        echo "Checking if Docker is available..."
        if ! command -v docker &> /dev/null; then
            echo -e "${RED}❌ Docker not found. Please install Docker first.${NC}"
            echo "   See: TESTCONTAINERS_GUIDE.md"
            exit 1
        fi
        
        if ! docker info &> /dev/null; then
            echo -e "${RED}❌ Docker daemon not running. Please start Docker.${NC}"
            exit 1
        fi
        
        echo -e "${GREEN}✓ Docker is available${NC}"
        echo ""
        echo "Running PostGIS tests with Testcontainers..."
        echo "⏳ First run may take 1-5 minutes to download PostGIS image..."
        echo ""
        
        run_test "PostGIS" "PostgreSQL with PostGIS via Testcontainers" \
            "TEST_USE_POSTGIS=true TEST_USE_TESTCONTAINERS=true ./gradlew test"
        ;;
        
    "both")
        echo "Running tests on both H2GIS and PostGIS..."
        echo ""
        
        # Run H2GIS tests
        run_test "H2GIS" "In-memory H2GIS database" \
            "./gradlew test"
        
        H2_RESULT=$?
        
        # Check Docker availability
        if ! command -v docker &> /dev/null || ! docker info &> /dev/null; then
            echo ""
            echo -e "${YELLOW}⚠️  Docker not available, skipping PostGIS tests${NC}"
            echo "   Install Docker to enable PostGIS testing"
            exit $H2_RESULT
        fi
        
        # Run PostGIS tests
        echo ""
        echo "⏳ Starting PostGIS tests (may take time on first run)..."
        
        run_test "PostGIS" "PostgreSQL with PostGIS via Testcontainers" \
            "TEST_USE_POSTGIS=true TEST_USE_TESTCONTAINERS=true ./gradlew test"
        
        PG_RESULT=$?
        
        # Summary
        echo ""
        echo "═══════════════════════════════════════════════════════════════"
        echo "  Test Summary"
        echo "═══════════════════════════════════════════════════════════════"
        
        if [ $H2_RESULT -eq 0 ]; then
            echo -e "  H2GIS:   ${GREEN}✅ PASSED${NC}"
        else
            echo -e "  H2GIS:   ${RED}❌ FAILED${NC}"
        fi
        
        if [ $PG_RESULT -eq 0 ]; then
            echo -e "  PostGIS: ${GREEN}✅ PASSED${NC}"
        else
            echo -e "  PostGIS: ${RED}❌ FAILED${NC}"
        fi
        
        echo "═══════════════════════════════════════════════════════════════"
        
        if [ $H2_RESULT -eq 0 ] && [ $PG_RESULT -eq 0 ]; then
            echo -e "${GREEN}🎉 All tests passed!${NC}"
            exit 0
        else
            exit 1
        fi
        ;;
        
    "quick")
        echo "Running quick test (single test class on H2GIS)..."
        run_test "Quick" "TestNoiseModelling on H2GIS" \
            "./gradlew test --tests '*TestNoiseModelling'"
        ;;
        
    "debug")
        echo "Running single test with debug output..."
        TEST_CLASS="${2:-TestNoiseModelling}"
        echo "Test class: ${TEST_CLASS}"
        
        run_test "Debug" "${TEST_CLASS} on H2GIS with debug output" \
            "./gradlew test --tests '*${TEST_CLASS}' --info"
        ;;
        
    "clean")
        echo "Cleaning test artifacts..."
        ./gradlew clean
        echo -e "${GREEN}✅ Cleaned${NC}"
        ;;
        
    *)
        echo "Usage: $0 [mode]"
        echo ""
        echo "Modes:"
        echo "  h2gis     - Run tests on H2GIS only (fast, default database)"
        echo "  postgis   - Run tests on PostGIS via Testcontainers (requires Docker)"
        echo "  both      - Run tests on both H2GIS and PostGIS (comprehensive)"
        echo "  quick     - Run single test class on H2GIS (fastest)"
        echo "  debug     - Run with debug output (usage: $0 debug [TestClassName])"
        echo "  clean     - Clean build artifacts"
        echo ""
        echo "Examples:"
        echo "  $0 h2gis              # Fast H2GIS tests"
        echo "  $0 postgis            # PostGIS with Testcontainers"
        echo "  $0 both               # Test both databases"
        echo "  $0 quick              # Quick smoke test"
        echo "  $0 debug TestReceivers # Debug specific test"
        echo ""
        echo "Requirements:"
        echo "  - Java 11+"
        echo "  - Docker (for postgis mode)"
        echo ""
        echo "See TESTCONTAINERS_GUIDE.md for detailed documentation"
        exit 1
        ;;
esac

echo ""
echo "═══════════════════════════════════════════════════════════════"
