package com.jpmc.midascore.kafka;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepo;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepo transactionRecordRepo;

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}" , groupId = "group-325")
    public void consume(Transaction transactions) {
        System.out.println("hello i am Consumer");
        System.out.println("Before");
        System.out.println(userRepository.findAll());



        logger.info("Received transaction: {}", transactions);

        UserRecord sender = userRepository.findById(transactions.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transactions.getRecipientId()).orElse(null);
        if(sender!=null && recipient!=null){
            if(sender.getBalance()>=transactions.getAmount()){
                sender.setBalance(sender.getBalance()-transactions.getAmount());
                recipient.setBalance(recipient.getBalance()+transactions.getAmount());

                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord transactionRecord = new TransactionRecord(sender,recipient,transactions.getAmount());
                transactionRecordRepo.save(transactionRecord);

                System.out.println("After");
                System.out.println(userRepository.findAll());
                System.out.println("successfully done the transaaction");
            }
        }

    }
}
