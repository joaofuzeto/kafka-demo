# Kafka Messaging Demo

Projeto desenvolvido para estudar e demonstrar, na prática, os principais conceitos de mensageria com **Apache Kafka e Spring Boot**.

A aplicação simula o processamento de pedidos por meio de eventos assíncronos, utilizando um serviço produtor e um serviço consumidor. O projeto também explora como o Kafka distribui mensagens entre partições e consumidores de um mesmo Consumer Group.

## Objetivos do projeto

- Implementar a publicação e o consumo de eventos com Spring Kafka.
- Entender a comunicação assíncrona entre serviços.
- Compreender Topics, Partitions e Offsets.
- Explorar Consumer Groups e distribuição de mensagens.
- Monitorar o progresso do consumo utilizando Consumer Group Lag.
- Entender como as Message Keys influenciam o particionamento.

## Tecnologias utilizadas

- Java
- Spring Boot
- Spring Kafka
- Apache Kafka
- Docker
- Maven

## Arquitetura

O projeto possui dois serviços principais:

- **order-api:** responsável por publicar eventos de pedidos no Kafka.
- **notification-service:** responsável por consumir e processar os eventos publicados.

Fluxo de comunicação:

```text
Client
  |
  v
order-api
  |
  | KafkaTemplate
  v
Kafka Topic: orders
  |
  +------ Partition 0
  +------ Partition 1
  +------ Partition 2
  |
  v
Consumer Group: notification-group
  |
  +------ OrderConsumer
  |
  +------ AuditConsumer
```

O diagrama representa a arquitetura conceitual após a criação das três partições. A distribuição efetiva depende da atribuição feita pelo Kafka e das mensagens publicadas.

## 1. Producer: publicação de eventos

O serviço `order-api` utiliza o `KafkaTemplate` do Spring Kafka para publicar eventos no tópico `orders`.

Exemplo simplificado do produtor:

```java
@Service
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(
            KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(OrderEvent event) {
        kafkaTemplate.send(
            "orders",
            event.customer(),
            event
        );

        System.out.println(
            "Order event sent to Kafka: " + event
        );
    }
}
```

A implementação utiliza o campo `customer` como chave da mensagem.

Os principais argumentos da publicação são:

- `orders`: tópico de destino.
- `event.customer()`: chave utilizada pelo particionador.
- `event`: conteúdo do evento publicado.

A utilização de uma chave permite que mensagens com a mesma chave sejam direcionadas à mesma partição pelo particionador padrão, desde que a configuração e a quantidade de partições permaneçam iguais.

## 2. Topic: `orders`

Um Topic é uma categoria lógica na qual o Kafka armazena os registros publicados pelos produtores.

Inicialmente, o tópico `orders` possuía uma única partição. Durante os testes, ele foi alterado para três partições.

Comando utilizado para consultar o tópico:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```

Comando utilizado para aumentar o número de partições:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic orders --partitions 3
```

Após a alteração, o tópico passou a ter:

| Configuração | Valor observado |
|---|---|
| Topic | `orders` |
| Quantidade de partições | 3 |
| Replication Factor | 1 |
| Broker líder observado | ID `1` |

O fator de replicação igual a 1 significa que cada partição possui apenas uma réplica configurada. Portanto, o ambiente de demonstração não oferece redundância de réplicas para tolerância à falha de um broker.

**Importante:** aumentar o número de partições não redistribui automaticamente os registros antigos entre as novas partições.

## 3. Consumers: consumo de eventos

O serviço `notification-service` possui dois listeners que demonstram diferentes comportamentos de consumo.

### OrderConsumer

```java
@Service
public class OrderConsumer {

    @KafkaListener(
        topics = "orders",
        groupId = "notification-group"
    )
    public void listen(OrderEvent event) {
        System.out.println(
            "Received order event: " + event
        );
    }
}
```

### AuditConsumer

O segundo listener utiliza o mesmo tópico e, no teste de distribuição, também pertence ao grupo `notification-group`. Sua função é demonstrar o processamento de auditoria.

Exemplo do formato observado nos logs:

```text
AUDIT - order received: OrderEvent[
    orderId=26,
    customer=JoaoTeste,
    product=Colher,
    price=21.95
]
```

Os dois listeners são consumidores do mesmo Consumer Group. Por isso, as partições são distribuídas entre eles, em vez de cada mensagem ser entregue automaticamente aos dois.

## 4. Consumer Groups

Um Consumer Group permite que múltiplos consumidores dividam o processamento das partições de um tópico.

Durante os testes, foram comparados dois cenários.

### Cenário A: grupos diferentes

Quando os consumidores pertencem a grupos diferentes, cada grupo mantém seu próprio progresso de consumo. Assim, ambos podem consumir os mesmos registros do tópico de maneira independente.

### Cenário B: mesmo grupo

Quando `OrderConsumer` e `AuditConsumer` utilizam o grupo `notification-group`, o Kafka atribui as partições entre os consumidores desse grupo.

Com três partições e dois consumidores, a distribuição observada foi:

| Consumidor | Partições atribuídas |
|---|---|
| `consumer-notification-group-1` | 0 e 1 |
| `consumer-notification-group-2` | 2 |

A distribuição exata depende da atribuição de partições realizada pelo Kafka. Ela pode mudar quando consumidores entram ou saem do grupo.

Dentro de um mesmo grupo, cada partição é atribuída a um consumidor por vez. Grupos diferentes, por outro lado, podem consumir os mesmos registros independentemente.

## 5. Offsets

Um Offset representa a posição de um registro dentro de uma partição.

Cada partição possui sua própria sequência de offsets. Portanto, o offset 5 da partição 0 não é o mesmo registro que o offset 5 da partição 1.

Comando utilizado para consultar o progresso do grupo:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notification-group
```

Os principais campos apresentados são:

| Campo | Significado |
|---|---|
| `CURRENT-OFFSET` | Posição de consumo confirmada pelo grupo, normalmente indicando o próximo offset a consumir. |
| `LOG-END-OFFSET` | Limite final do log da partição. |
| `LAG` | Diferença entre o limite do log e o progresso confirmado do grupo. |
| `PARTITION` | Identificador da partição. |
| `CONSUMER-ID` | Identificador do consumidor associado à partição. |

### Teste de Lag

Foi realizado um teste interrompendo temporariamente o serviço consumidor, publicando novos pedidos e verificando o progresso do grupo.

O teste demonstrou que:

1. Enquanto o consumidor estava parado, novos registros continuaram sendo publicados no tópico.
2. O progresso de consumo do grupo ficou para trás em relação ao final do log.
3. Após reiniciar o consumidor, os registros pendentes foram processados.
4. O Lag voltou a zero após o consumo dos registros disponíveis naquele momento.

O Lag permite acompanhar o atraso de consumo, mas um valor igual a zero, isoladamente, não garante que todo o sistema esteja funcionando corretamente.

## 6. Consultando os offsets das partições

Para consultar o limite de offsets de todas as partições do tópico:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
```

Durante os testes, foram observados resultados como:

```text
orders:0:12
orders:1:8
orders:2:0
```

Cada linha apresenta o tópico, a partição e o limite final de offsets.

Nesse exemplo, a partição 0 possuía limite 12, a partição 1 possuía limite 8 e a partição 2 possuía limite 0.

Esses valores representam limites de offsets, não necessariamente a quantidade de mensagens ainda não consumidas.

## 7. Partitions e distribuição de mensagens

As partições permitem dividir o log de um tópico em sequências independentes. Elas também possibilitam que consumidores de um mesmo grupo processem diferentes partes do tópico em paralelo.

Durante os testes, o tópico `orders` foi aumentado de uma para três partições.

A quantidade de partições influencia o paralelismo possível dentro de um Consumer Group. Entretanto, adicionar partições não garante que as mensagens sejam distribuídas igualmente entre elas.

A distribuição depende de fatores como:

- A chave de cada mensagem.
- O particionador utilizado pelo produtor.
- A quantidade de partições.
- A configuração do produtor.

## 8. Message Keys

Uma Message Key é um valor associado ao registro publicado no Kafka. Ela pode ser utilizada pelo particionador para determinar a partição de destino.

Inicialmente, o produtor enviava mensagens sem uma chave explícita:

```java
kafkaTemplate.send("orders", event);
```

Posteriormente, a publicação foi alterada para utilizar o cliente como chave:

```java
kafkaTemplate.send(
    "orders",
    event.customer(),
    event
);
```

### Teste com a mesma chave

Foram enviados três pedidos com o cliente `JoaoTeste`.

Resultado observado:

```text
orders:0:12
orders:1:8
orders:2:0
```

Os três novos registros foram direcionados à partição 1, cujo limite passou de 5 para 8.

### Teste com chaves diferentes

Em seguida, foram enviados pedidos com os clientes `JoaoTeste` e `MariaTeste`.

Resultado observado:

```text
orders:0:12
orders:1:9
orders:2:1
```

O pedido de `JoaoTeste` foi direcionado à partição 1, enquanto o de `MariaTeste` foi direcionado à partição 2.

Os testes demonstraram que:

- Mensagens com a mesma chave foram direcionadas à mesma partição.
- Chaves diferentes puderam resultar em partições diferentes.
- Clientes diferentes não possuem garantia de receber partições exclusivas.
- A ordem dos registros é garantida dentro de uma partição, não como uma ordenação global entre todas as partições.

## 9. Configuração do consumidor

O serviço `notification-service` utiliza o endereço do broker e configurações de desserialização para interpretar os eventos recebidos.

Exemplo das propriedades utilizadas durante os testes:

```properties
spring.application.name=notification-service

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer

spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.properties.spring.json.value.default.type=br.com.fuzeto.notificationservice.OrderEvent
spring.kafka.consumer.properties.spring.json.use.type.headers=false
```

A configuração `auto-offset-reset=earliest` é utilizada quando o grupo não possui um offset válido previamente confirmado para a partição. Ela não significa que o consumidor sempre começará do início em toda inicialização.

A propriedade `spring.json.use.type.headers=false` foi utilizada para evitar que o consumidor dependesse do cabeçalho de tipo produzido por uma classe de outro pacote. O tipo padrão define a classe `OrderEvent` utilizada pelo serviço consumidor.

**Nota de configuração:** `spring.kafka.consumer.properties.spring.json.trusted.packages=*` amplia a lista de pacotes confiáveis do desserializador. Em aplicações reais, é preferível restringi-la aos pacotes necessários.

## 10. Executando os testes

Pré-requisitos:

- Java instalado.
- Docker instalado e funcionando.
- Kafka iniciado no container `kafka`.
- Os serviços `order-api` e `notification-service` configurados localmente.

### Verificar o container

```powershell
docker ps
```

Caso o container esteja parado:

```powershell
docker start kafka
```

### Consultar o tópico

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```

### Consultar o Consumer Group

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notification-group
```

### Consultar os offsets

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
```

Inicie os serviços conforme a configuração local do projeto e publique pedidos utilizando o endpoint já implementado na API. Os resultados podem ser acompanhados pelos logs do consumidor e pelos comandos de inspeção do Kafka.

## Aprendizados

Este projeto permitiu explorar os seguintes conceitos na prática:

- Comunicação assíncrona com Apache Kafka.
- Publicação de eventos utilizando Spring Kafka.
- Consumo de mensagens com `@KafkaListener`.
- Diferença entre consumidores do mesmo grupo e de grupos diferentes.
- Funcionamento de offsets e acompanhamento de Consumer Group Lag.
- Criação e distribuição de partições.
- Influência das chaves na seleção da partição.
- Relação entre partições, paralelismo e ordenação dos registros.

## Próximos passos

- Estudar replicação de partições e tolerância a falhas.
- Explorar tratamento de falhas e políticas de retry.
- Investigar Dead Letter Topics (DLT).
- Avaliar estratégias de idempotência e processamento confiável.
- Aprimorar a documentação e adicionar diagramas de arquitetura.

---

**Projeto:** desenvolvido para aprofundar os conhecimentos em Apache Kafka, mensageria e arquitetura de serviços com Java e Spring Boot.
