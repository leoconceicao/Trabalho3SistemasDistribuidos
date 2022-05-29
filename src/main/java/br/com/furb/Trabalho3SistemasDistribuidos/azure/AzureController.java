/**
 * @author 	Breno Spagnolo da Rosa
 * 			Joao Vitor Krueger Moutinho
 * 			Leonardo Cognacco Conceicao
 */
package br.com.furb.Trabalho3SistemasDistribuidos.azure;

import com.azure.messaging.servicebus.*;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.swing.JOptionPane;

public class AzureController {

	// Código de string buscado na Fila de mensagens (Cadeia de Conexão Primária ou Secundária em Fila de Mensagens > Políticas de Acesso Compartilhado)
	private static String connectionString = "Endpoint=sb://serverlecpocteste.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tRr6QsJg+JBhUAmGyW8GMy8J7PNrPWmsYzcmA+troNY=";
	private static String fila = "filapoctrabalho3"; // Fila criada no Azure

	public static String getConnectionString() {
		return connectionString;
	}

	public static String getFila() {
		return fila;
	}

	public static void enviarMensagens(ArrayList<ServiceBusMessage> mensagens) {
		// Criar a conexão com o Service Bus para a fila
		ServiceBusSenderClient senderClient = new ServiceBusClientBuilder().connectionString(getConnectionString())
				.sender().queueName(getFila()).buildClient();

		// Criar uma lista de mensagens
		ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();

		for (ServiceBusMessage message : mensagens) {
			if (messageBatch.tryAddMessage(message)) {
				continue;
			}

			// Em caso da lista estar cheia, as mensagens sao enviadas e sera criado um novo
			// pacote para o envio das demais mensagens
			senderClient.sendMessages(messageBatch);
			System.out.println("Enviado uma lista de mensagens para a fila: " + getFila());

			// Criar um novo pacote de mensagens
			messageBatch = senderClient.createMessageBatch();

			// Adicionar mensagem nao possivel anteriormente
			if (!messageBatch.tryAddMessage(message)) {
				System.err.printf("Mensagem é muito grande para uma lista vazia. Tamanho máxio: %s.",
						messageBatch.getMaxSizeInBytes());
			}
		}

		if (messageBatch.getCount() > 0) {
			senderClient.sendMessages(messageBatch);
			System.out.println("Enviado uma lista de mensagens para a fila: " + getFila());
		}
	}

	public static void receiveMessages() throws InterruptedException {
		CountDownLatch countdownLatch = new CountDownLatch(1);

		// Criar uma instância de processador pelo cliente do Service Bus
		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder().connectionString(connectionString)
				.processor().queueName(getFila()).processMessage(AzureController::processMessage)
				.processError(context -> processError(context, countdownLatch)).buildProcessorClient();

		System.out.println("Iniciando o processador");
		processorClient.start();
	}

	public static void enviarMensagem(String mensagem) {
		// Criar a conexão com o Service Bus
		ServiceBusSenderClient senderClient = new ServiceBusClientBuilder().connectionString(getConnectionString())
				.sender().queueName(getFila()).buildClient();

		// Enviar uma mensagem para a fila
		senderClient.sendMessage(new ServiceBusMessage(mensagem));
		System.out.println("Enviado uma única mensagem para a fila: " + getFila());
	}

	private static void processMessage(ServiceBusReceivedMessageContext contexto) {
		
		ServiceBusReceivedMessage mensagem = contexto.getMessage();
		System.out.printf("Processada mensagem. Sessão: %s, Sequência #: %s. Conteúdo da mensagem: %s%n", mensagem.getMessageId(),
				mensagem.getSequenceNumber(), mensagem.getBody());
		JOptionPane.showMessageDialog(null, String.format("Mensagem recebida do Azure! Sequência da Mensagem: %s. Conteúdo da Mensagem: %s",
				mensagem.getSequenceNumber(), mensagem.getBody()));
	}

	private static void processError(ServiceBusErrorContext contexto, CountDownLatch countdownLatch) {
		System.out.printf("Erro ao buscar informações da fila: '%s'. Entidade: '%s'%n",
				contexto.getFullyQualifiedNamespace(), contexto.getEntityPath());

		if (!(contexto.getException() instanceof ServiceBusException)) {
			System.out.printf("Non-ServiceBusException occurred: %s%n", contexto.getException());
			return;
		}

		ServiceBusException excecao = (ServiceBusException) contexto.getException();
		ServiceBusFailureReason razao = excecao.getReason();

		if (razao == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
				|| razao == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
				|| razao == ServiceBusFailureReason.UNAUTHORIZED) {
			System.out.printf("Um erro ocorreu na execução. Parando processo pelo motivo %s: %s%n", razao,
					excecao.getMessage());

			countdownLatch.countDown();
		} else if (razao == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
			System.out.printf("Lock de mensagem perdido pelo motivo: %s%n", contexto.getException());
		} else if (razao == ServiceBusFailureReason.SERVICE_BUSY) {
			try {
				// Escolhe um valor arbitrário para aguardar e tentar novamente.
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				System.err.println("Não foi possível aguardar o período escolhido");
			}
		} else {
			System.out.printf("Erro da fonte %s, motivo %s, mensagem: %s%n", contexto.getErrorSource(), razao,
					contexto.getException());
		}
	}

	// Metodo para formatacao do Hashmap para um JSON, para envio ao Azure e
	// manipulacao em Tarefa
	public static String criarJSONdeHashMap(HashMap<String, Object> valores) {
		return new JSONObject(valores).toString();
	}

	// Criacao de mensagem com String JSON para envio ao Azure e manipulacao em
	// Tarefa
	public static ServiceBusMessage criarMensagemJSON(String mensagem) {
		return new ServiceBusMessage(mensagem);
	}
}
