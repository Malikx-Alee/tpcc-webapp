"""
CockroachDB Database Connector - STUDY IMPLEMENTATION SKELETON
Participants will implement this connector to integrate with CockroachDB

This file contains TODO items that participants need to complete during the study.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from .base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class CockroachConnector(BaseDatabaseConnector):
    """
    CockroachDB database connector for TPC-C application

    Participants will implement connection management and query execution
    for CockroachDB during the UX study.
    """

    def __init__(self):
        """
        Initialize CockroachDB connection

        TODO: Implement CockroachDB connection initialization
        - Read configuration from environment variables
        - Set up PostgreSQL-compatible connection to CockroachDB
        - Configure connection parameters and SSL settings
        - Handle CockroachDB-specific connection requirements

        Environment variables to use:
        - COCKROACH_CONNECTION_STRING: PostgreSQL connection string for CockroachDB
        """
        super().__init__()
        self.provider_name = "CockroachDB"

        # Initialize CockroachDB connection
        self.connection = None

        # Read configuration from environment
        self.connection_string = os.getenv("COCKROACH_CONNECTION_STRING")

        # Validate required configuration
        if not self.connection_string:
            logger.error("COCKROACH_CONNECTION_STRING environment variable is required")
            raise ValueError("COCKROACH_CONNECTION_STRING environment variable is required")

        # Initialize PostgreSQL client and connection
        try:
            logger.info(f"Initializing CockroachDB connection to: {self._mask_connection_string()}")
            self.connection = psycopg2.connect(
                self.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor,
                connect_timeout=10
            )
            # Set autocommit for CockroachDB compatibility
            self.connection.autocommit = True
            logger.info("✅ CockroachDB connection initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize CockroachDB connection: {str(e)}")
            self.connection = None
            raise

    def _mask_connection_string(self) -> str:
        """Mask sensitive information in connection string for logging"""
        if not self.connection_string:
            return "None"
        # Replace password with asterisks
        import re
        masked = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', self.connection_string)
        return masked

    def test_connection(self) -> bool:
        """
        Test connection to CockroachDB database

        TODO: Implement connection testing
        - Test connection to CockroachDB cluster
        - Execute a simple query to verify connectivity
        - Return True if successful, False otherwise
        - Log connection status for study data collection

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if not self.connection:
                logger.warning("No connection available for testing")
                return False

            # Test connection to CockroachDB cluster
            with self.connection.cursor() as cursor:
                # Execute a simple query to verify connectivity
                start_time = time.time()
                cursor.execute("SELECT 1 as test_value")
                result = cursor.fetchone()
                end_time = time.time()

                # Log connection status for study data collection
                response_time = (end_time - start_time) * 1000  # Convert to milliseconds
                logger.info(f"✅ CockroachDB connection test successful (response time: {response_time:.2f}ms)")
                logger.info(f"   Test query result: {result}")

                return result is not None and result['test_value'] == 1

        except Exception as e:
            logger.error(f"❌ CockroachDB connection test failed: {str(e)}")
            return False

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL query on CockroachDB

        TODO: Implement query execution
        - Handle parameterized queries safely
        - Convert CockroachDB results to standard format
        - Handle CockroachDB-specific data types
        - Implement proper error handling
        - Log query performance for study metrics

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            List of dictionaries representing query results
        """
        try:
            if not self.connection:
                logger.error("No database connection available")
                raise RuntimeError("No database connection available")

            # Log query performance for study metrics
            start_time = time.time()
            logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
            if params:
                logger.debug(f"Query parameters: {params}")

            with self.connection.cursor() as cursor:
                # Handle parameterized queries safely
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Convert CockroachDB results to standard format
                # Fetch results for SELECT queries and queries with RETURNING clause
                formatted_results = []
                query_upper = query.strip().upper()
                if query_upper.startswith('SELECT') or 'RETURNING' in query_upper:
                    results = cursor.fetchall()

                    # Handle CockroachDB-specific data types
                    # Convert RealDictRow objects to regular dictionaries
                    for row in results:
                        if row:
                            # Convert to regular dict and handle special data types
                            row_dict = dict(row)
                            # Convert any datetime objects to ISO format strings for JSON serialization
                            for key, value in row_dict.items():
                                if hasattr(value, 'isoformat'):  # datetime objects
                                    row_dict[key] = value.isoformat()
                                elif isinstance(value, (bytes, bytearray)):  # binary data
                                    row_dict[key] = value.decode('utf-8', errors='ignore')
                            formatted_results.append(row_dict)
                else:
                    # For non-SELECT queries without RETURNING (INSERT, UPDATE, DELETE, CREATE, etc.)
                    # Return empty list but log the row count if available
                    if hasattr(cursor, 'rowcount') and cursor.rowcount >= 0:
                        logger.debug(f"Query affected {cursor.rowcount} rows")

                end_time = time.time()
                execution_time = (end_time - start_time) * 1000  # Convert to milliseconds

                # Log performance metrics
                logger.info(f"✅ Query executed successfully in {execution_time:.2f}ms, returned {len(formatted_results)} rows")

                return formatted_results

        except Exception as e:
            logger.error(f"❌ CockroachDB query execution failed: {str(e)}")
            logger.error(f"   Query: {query}")
            if params:
                logger.error(f"   Parameters: {params}")
            raise

    def execute_in_transaction(self, operations: List[tuple]) -> List[Dict[str, Any]]:
        """
        Execute multiple operations in a single transaction

        Args:
            operations: List of tuples (query, params) to execute in transaction

        Returns:
            List of results from each operation

        Raises:
            Exception: If any operation fails, entire transaction is rolled back
        """
        try:
            if not self.connection:
                logger.error("No database connection available")
                raise RuntimeError("No database connection available")

            # Disable autocommit for transaction
            original_autocommit = self.connection.autocommit
            self.connection.autocommit = False

            results = []

            try:
                with self.connection.cursor() as cursor:
                    for query, params in operations:
                        logger.debug(f"Executing in transaction: {query[:100]}{'...' if len(query) > 100 else ''}")

                        if params:
                            cursor.execute(query, params)
                        else:
                            cursor.execute(query)

                        # Fetch results if it's a SELECT query
                        if query.strip().upper().startswith('SELECT'):
                            operation_results = cursor.fetchall()
                            formatted_results = []
                            for row in operation_results:
                                if row:
                                    row_dict = dict(row)
                                    for key, value in row_dict.items():
                                        if hasattr(value, 'isoformat'):
                                            row_dict[key] = value.isoformat()
                                        elif isinstance(value, (bytes, bytearray)):
                                            row_dict[key] = value.decode('utf-8', errors='ignore')
                                    formatted_results.append(row_dict)
                            results.append(formatted_results)
                        else:
                            results.append([])

                # Commit the transaction
                self.connection.commit()
                logger.info(f"✅ Transaction committed successfully with {len(operations)} operations")

            except Exception as e:
                # Rollback on any error
                self.connection.rollback()
                logger.info(f"🔄 Transaction rolled back due to error: {str(e)}")
                raise
            finally:
                # Restore original autocommit setting
                self.connection.autocommit = original_autocommit

            return results

        except Exception as e:
            logger.error(f"❌ Transaction execution failed: {str(e)}")
            raise

    def get_provider_name(self) -> str:
        """Return the provider name"""
        return self.provider_name

    def execute_new_order(
        self, warehouse_id: int, district_id: int, customer_id: int, items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute TPC-C New Order transaction

        Args:
            warehouse_id: Warehouse ID
            district_id: District ID
            customer_id: Customer ID
            items: List of items with item_id, supply_warehouse_id, quantity

        Returns:
            Dict with success status and order details
        """
        try:
            from datetime import datetime

            logger.info(f"🛒 Executing New Order transaction: W={warehouse_id}, D={district_id}, C={customer_id}")

            # Generate order ID by getting next district order ID
            next_order_query = """
                UPDATE district
                SET d_next_o_id = d_next_o_id + 1
                WHERE d_w_id = %s AND d_id = %s
                RETURNING d_next_o_id - 1 as order_id
            """

            order_id_result = self.execute_query(next_order_query, (warehouse_id, district_id))
            if not order_id_result:
                return {"success": False, "error": "Failed to generate order ID"}

            order_id = order_id_result[0]['order_id']

            # Prepare transaction operations
            operations = []

            # Insert into order table
            order_entry_date = datetime.now()
            all_local = 1 if all(item.get('supply_warehouse_id', warehouse_id) == warehouse_id for item in items) else 0

            insert_order_query = """
                INSERT INTO "order" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            operations.append((insert_order_query, (order_id, district_id, warehouse_id, customer_id, order_entry_date, len(items), all_local)))

            # Insert into new_order table
            insert_new_order_query = """
                INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
                VALUES (%s, %s, %s)
            """
            operations.append((insert_new_order_query, (order_id, district_id, warehouse_id)))

            # Process each order line
            total_amount = 0.0
            order_lines = []

            for line_number, item in enumerate(items, 1):
                item_id = item['item_id']
                supply_warehouse_id = item.get('supply_warehouse_id', warehouse_id)
                quantity = item['quantity']

                # Get item information
                item_query = """
                    SELECT i_price, i_name, i_data
                    FROM item
                    WHERE i_id = %s
                """
                item_result = self.execute_query(item_query, (item_id,))
                if not item_result:
                    return {"success": False, "error": f"Item {item_id} not found"}

                item_price = item_result[0]['i_price']
                item_name = item_result[0]['i_name']

                # Update stock
                stock_update_query = """
                    UPDATE stock
                    SET s_quantity = CASE
                        WHEN s_quantity >= %s THEN s_quantity - %s
                        ELSE s_quantity + 91 - %s
                    END,
                    s_ytd = s_ytd + %s,
                    s_order_cnt = s_order_cnt + 1
                    WHERE s_i_id = %s AND s_w_id = %s
                    RETURNING s_quantity, s_dist_01 as s_dist_info
                """
                operations.append((stock_update_query, (quantity, quantity, quantity, quantity, item_id, supply_warehouse_id)))

                # Calculate line amount
                line_amount = quantity * float(item_price)
                total_amount += line_amount

                # Insert order line
                insert_order_line_query = """
                    INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                operations.append((insert_order_line_query, (order_id, district_id, warehouse_id, line_number, item_id, supply_warehouse_id, quantity, line_amount, f"dist_info_{line_number}")))

                order_lines.append({
                    'ol_number': line_number,
                    'ol_i_id': item_id,
                    'ol_supply_w_id': supply_warehouse_id,
                    'ol_quantity': quantity,
                    'ol_amount': line_amount,
                    'i_name': item_name
                })

            # Execute all operations in a transaction
            self.execute_in_transaction(operations)

            logger.info(f"✅ New Order transaction completed: Order ID {order_id}, Total: ${total_amount:.2f}")

            return {
                "success": True,
                "order_id": order_id,
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "total_amount": total_amount,
                "order_lines": order_lines,
                "entry_date": order_entry_date.isoformat()
            }

        except Exception as e:
            logger.error(f"❌ New Order transaction failed: {str(e)}")
            return {"success": False, "error": str(e)}

    def execute_payment(
        self, warehouse_id: int, district_id: int, customer_id: int, amount: float
    ) -> Dict[str, Any]:
        """
        Execute TPC-C Payment transaction

        Args:
            warehouse_id: Warehouse ID
            district_id: District ID
            customer_id: Customer ID
            amount: Payment amount

        Returns:
            Dict with success status and payment details
        """
        try:
            from datetime import datetime

            logger.info(f"💳 Executing Payment transaction: W={warehouse_id}, D={district_id}, C={customer_id}, Amount=${amount}")

            operations = []

            # Update warehouse YTD
            update_warehouse_query = """
                UPDATE warehouse
                SET w_ytd = w_ytd + %s
                WHERE w_id = %s
                RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip
            """
            operations.append((update_warehouse_query, (amount, warehouse_id)))

            # Update district YTD
            update_district_query = """
                UPDATE district
                SET d_ytd = d_ytd + %s
                WHERE d_w_id = %s AND d_id = %s
                RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip
            """
            operations.append((update_district_query, (amount, warehouse_id, district_id)))

            # Update customer balance and payment info
            update_customer_query = """
                UPDATE customer
                SET c_balance = c_balance - %s,
                    c_ytd_payment = c_ytd_payment + %s,
                    c_payment_cnt = c_payment_cnt + 1
                WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
                RETURNING c_first, c_middle, c_last, c_balance, c_credit
            """
            operations.append((update_customer_query, (amount, amount, warehouse_id, district_id, customer_id)))

            # Insert history record
            history_date = datetime.now()
            insert_history_query = """
                INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            history_data = f"Payment W:{warehouse_id} D:{district_id} C:{customer_id}"
            operations.append((insert_history_query, (customer_id, district_id, warehouse_id, district_id, warehouse_id, history_date, amount, history_data)))

            # Execute all operations in a transaction
            results = self.execute_in_transaction(operations)

            logger.info(f"✅ Payment transaction completed: ${amount} for customer {customer_id}")

            return {
                "success": True,
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "customer_id": customer_id,
                "amount": amount,
                "payment_date": history_date.isoformat(),
                "customer_balance": results[2][0]['c_balance'] if results and len(results) > 2 and results[2] else None
            }

        except Exception as e:
            logger.error(f"❌ Payment transaction failed: {str(e)}")
            return {"success": False, "error": str(e)}

    def execute_delivery(self, warehouse_id: int, carrier_id: int) -> Dict[str, Any]:
        """
        Execute TPC-C Delivery transaction

        Args:
            warehouse_id: Warehouse ID
            carrier_id: Carrier ID

        Returns:
            Dict with success status and delivery details
        """
        try:
            from datetime import datetime

            logger.info(f"🚚 Executing Delivery transaction: W={warehouse_id}, Carrier={carrier_id}")

            delivery_date = datetime.now()
            delivered_orders = []

            # Process delivery for each district in the warehouse
            for district_id in range(1, 11):  # TPC-C has 10 districts per warehouse
                try:
                    # Find oldest undelivered order
                    find_order_query = """
                        SELECT no_o_id
                        FROM new_order
                        WHERE no_w_id = %s AND no_d_id = %s
                        ORDER BY no_o_id
                        LIMIT 1
                    """

                    order_result = self.execute_query(find_order_query, (warehouse_id, district_id))

                    if order_result:
                        order_id = order_result[0]['no_o_id']

                        operations = []

                        # Delete from new_order
                        delete_new_order_query = """
                            DELETE FROM new_order
                            WHERE no_w_id = %s AND no_d_id = %s AND no_o_id = %s
                        """
                        operations.append((delete_new_order_query, (warehouse_id, district_id, order_id)))

                        # Update order with carrier
                        update_order_query = """
                            UPDATE "order"
                            SET o_carrier_id = %s
                            WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s
                            RETURNING o_c_id
                        """
                        operations.append((update_order_query, (carrier_id, warehouse_id, district_id, order_id)))

                        # Update order lines with delivery date and get total amount
                        update_order_lines_query = """
                            UPDATE order_line
                            SET ol_delivery_d = %s
                            WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s
                            RETURNING ol_amount
                        """
                        operations.append((update_order_lines_query, (delivery_date, warehouse_id, district_id, order_id)))

                        # Execute operations for this district
                        results = self.execute_in_transaction(operations)

                        # Calculate total amount and update customer balance
                        if results and len(results) >= 3:
                            customer_id = results[1][0]['o_c_id'] if results[1] else None
                            order_line_amounts = results[2] if results[2] else []
                            total_amount = sum(float(line['ol_amount']) for line in order_line_amounts)

                            if customer_id and total_amount > 0:
                                # Update customer balance
                                update_customer_query = """
                                    UPDATE customer
                                    SET c_balance = c_balance + %s,
                                        c_delivery_cnt = c_delivery_cnt + 1
                                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
                                """
                                self.execute_query(update_customer_query, (total_amount, warehouse_id, district_id, customer_id))

                                delivered_orders.append({
                                    'district_id': district_id,
                                    'order_id': order_id,
                                    'customer_id': customer_id,
                                    'total_amount': total_amount
                                })

                except Exception as e:
                    logger.warning(f"Failed to deliver order in district {district_id}: {str(e)}")
                    continue

            logger.info(f"✅ Delivery transaction completed: {len(delivered_orders)} orders delivered")

            return {
                "success": True,
                "warehouse_id": warehouse_id,
                "carrier_id": carrier_id,
                "delivery_date": delivery_date.isoformat(),
                "delivered_orders": delivered_orders,
                "orders_delivered": len(delivered_orders)
            }

        except Exception as e:
            logger.error(f"❌ Delivery transaction failed: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_stock_level(self, warehouse_id: int, district_id: int, threshold: int) -> Dict[str, Any]:
        """
        Execute TPC-C Stock Level transaction

        Args:
            warehouse_id: Warehouse ID
            district_id: District ID
            threshold: Stock level threshold

        Returns:
            Dict with success status and stock level count
        """
        try:
            logger.info(f"📦 Executing Stock Level transaction: W={warehouse_id}, D={district_id}, Threshold={threshold}")

            # Get the next order ID for the district
            next_order_query = """
                SELECT d_next_o_id
                FROM district
                WHERE d_w_id = %s AND d_id = %s
            """

            next_order_result = self.execute_query(next_order_query, (warehouse_id, district_id))
            if not next_order_result:
                return {"success": False, "error": "District not found"}

            next_order_id = next_order_result[0]['d_next_o_id']

            # Count items with stock below threshold from recent orders
            stock_level_query = """
                SELECT COUNT(DISTINCT s.s_i_id) as low_stock_count
                FROM stock s
                JOIN order_line ol ON s.s_i_id = ol.ol_i_id AND s.s_w_id = ol.ol_supply_w_id
                WHERE ol.ol_w_id = %s
                  AND ol.ol_d_id = %s
                  AND ol.ol_o_id >= %s - 20
                  AND ol.ol_o_id < %s
                  AND s.s_quantity < %s
            """

            stock_result = self.execute_query(stock_level_query, (warehouse_id, district_id, next_order_id, next_order_id, threshold))

            low_stock_count = stock_result[0]['low_stock_count'] if stock_result else 0

            logger.info(f"✅ Stock Level transaction completed: {low_stock_count} items below threshold")

            return {
                "success": True,
                "warehouse_id": warehouse_id,
                "district_id": district_id,
                "threshold": threshold,
                "low_stock_count": low_stock_count
            }

        except Exception as e:
            logger.error(f"❌ Stock Level transaction failed: {str(e)}")
            return {"success": False, "error": str(e)}

    def close_connection(self):
        """
        Close database connections

        TODO: Implement connection cleanup
        - Close PostgreSQL client connections
        - Clean up any connection pools
        - Log connection closure for study metrics
        """
        try:
            # Implement connection cleanup
            if self.connection:
                # Close PostgreSQL client connections
                self.connection.close()
                self.connection = None
                # Log cleanup completion
                logger.info("✅ CockroachDB connection closed successfully")
            else:
                logger.debug("No connection to close")
        except Exception as e:
            logger.error(f"❌ Connection cleanup failed: {str(e)}")
            # Ensure connection is set to None even if close fails
            self.connection = None
