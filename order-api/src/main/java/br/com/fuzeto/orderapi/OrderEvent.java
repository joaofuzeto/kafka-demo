package br.com.fuzeto.orderapi;

public record OrderEvent(
   Long orderId,
   String customer,
   String product,
   Double price
) {}
