/**
 * @author 	Breno Spagnolo da Rosa
 * 			Joao Vitor Krueger Moutinho
 * 			Leonardo Cognacco Conceicao
 */

package br.com.furb.Trabalho3SistemasDistribuidos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.azure.messaging.servicebus.ServiceBusMessage;

import br.com.furb.Trabalho3SistemasDistribuidos.azure.AzureController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Principal {

	private static Scanner scan = new Scanner(System.in);

	public static Scanner getScan() {
		return scan;
	}

	public static void main(String[] args) {
		criaProdutos();
	}

	public static void criaProdutos() {
		String produto = "", jsonMensagem;
		int qtdConexoes = 0;
		double preco = 0d, estoque = 0d;
		HashMap<String, Object> valores = new HashMap<>(3);
		ArrayList<ServiceBusMessage> mensagens = new ArrayList<>();
		try {
			System.out.println("Digite a quantidade de registros a inserir:");
			qtdConexoes = getScan().nextInt();
			if (qtdConexoes > 0) {
				for (int i = 0; i < qtdConexoes; i++) {
					System.out.println("Digite o produto desejado:");
					produto = getScan().next();

					System.out.println("Digite o preço desejado para o produto: (Ex.: 2,5)");
					preco = getScan().nextDouble();

					System.out.println("Digite o estoque do produto desejado: ");
					estoque = getScan().nextDouble();

					valores.put("Produto", produto);
					valores.put("Preço", preco);
					valores.put("Estoque", estoque);

					// Criar mensagem JSON em String para apresentacao em tela, e criacao das
					// mensagens para envio ao Azure
					jsonMensagem = AzureController.criarJSONdeHashMap(valores);
					System.out.println(AzureController.criarJSONdeHashMap(valores));
					mensagens.add(AzureController.criarMensagemJSON(jsonMensagem));
				}
				AzureController.enviarMensagens(mensagens);
			}
		} catch (Exception e) {
			// Em caso de erro na insercao de algum dos produtos, e gerado uma mensagem para
			// o azure e enviado para a fila, como um log da insercao com o erro que foi
			// gerado
			HashMap<String, Object> erro = new HashMap<>(1);
			erro.put("Erro", "Erro na digitação do produto: " + e.toString());
			AzureController.enviarMensagem(AzureController.criarJSONdeHashMap(erro));
		}
	}
}