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

public class AzureController {

	private static String connectionString = "Endpoint=sb://serverlecpocteste.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tRr6QsJg+JBhUAmGyW8GMy8J7PNrPWmsYzcmA+troNY=";
	private static String fila = "filapoctrabalho3";

	public static String getConnectionString() {
		return connectionString;
	}

	public static String getFila() {
		return fila;
	}

	public static void enviarMensagens(ArrayList<ServiceBusMessage> mensagens) {
		// create a Service Bus Sender client for the queue
		ServiceBusSenderClient senderClient = new ServiceBusClientBuilder().connectionString(getConnectionString())
				.sender().queueName(getFila()).buildClient();

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

		// Fechar a conexão com o Service Bus
		senderClient.close();
	}

	public static void enviarMensagem(String mensagem) {
		// Criar a conexão com o Service Bus
		ServiceBusSenderClient senderClient = new ServiceBusClientBuilder().connectionString(getConnectionString())
				.sender().queueName(getFila()).buildClient();

		// Enviar uma mensagem para a fila
		senderClient.sendMessage(new ServiceBusMessage(mensagem));
		System.out.println("Sent a single message to the queue: " + getFila());

		// Fechar a conexão com o Service Bus
		senderClient.close();
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
