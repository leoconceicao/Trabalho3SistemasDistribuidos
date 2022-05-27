package br.com.furb.Trabalho3SistemasDistribuidos;

import br.com.furb.Trabalho3SistemasDistribuidos.azure.AzureController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

public class PrincipalReceberMensagens {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		AzureController.receiveMessages();
	}

}
