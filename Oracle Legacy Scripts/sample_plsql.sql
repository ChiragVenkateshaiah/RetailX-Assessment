
DECLARE
  v_total NUMBER;
BEGIN
  SELECT SUM(amount) INTO v_total FROM orders;
  DBMS_OUTPUT.PUT_LINE('Total Amount: ' || v_total);
END;
/
