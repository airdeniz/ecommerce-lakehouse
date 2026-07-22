{#
  Single source of truth for order-status semantics, shared by core_orders and
  the marts so the "what counts as revenue" rule lives in exactly one place.

  Full lifecycle (current-state, stored in orders.status):
    CREATED -> PAID -> PREPARING -> SHIPPED -> DELIVERED   (happy path)
    CREATED -> CANCELLED                                   (unpaid cancellation)
    PAID/PREPARING -> REFUNDED                             (paid then cancelled + refunded)
    DELIVERED -> RETURNED -> REFUNDED                      (return flow)

  revenue_statuses() = the states where money is currently recognised: the order
  has been paid and the sale has not been reversed. Post-payment fulfilment
  states (PREPARING/SHIPPED/DELIVERED) are still paid revenue. CREATED is unpaid,
  CANCELLED is an unpaid cancellation, and RETURNED/REFUNDED reverse the sale, so
  none of those count.
#}
{% macro revenue_statuses() %}('PAID', 'PREPARING', 'SHIPPED', 'DELIVERED'){% endmacro %}
