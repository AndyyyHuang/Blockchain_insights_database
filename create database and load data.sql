# 建立数据库

CREATE DATABASE DEX;
use DEX;


CREATE TABLE token(
token CHAR(10),
price DECIMAL(30,10),
volumn_24h DECIMAL(30,2),
primary key(token)
)AUTO_INCREMENT=1;


CREATE TABLE transaction(
token CHAR(10) NOT NULL,
token_address varchar(256) NOT NULL,
network CHAR(10),
side CHAR(10),
amount DECIMAL(30,10),
address varchar(256),
price DECIMAL(30,20),
time_stamp timestamp,
transaction_id varchar(256),
primary key(transaction_id)
)AUTO_INCREMENT=1;


CREATE TABLE account(
address CHAR(64),
token CHAR(20),
position DECIMAL(30,10),
primary key(address, token)
)auto_increment=1;


# 将数据写入数据库中
load data infile 'transaction.csv' into table transaction fields terminated by ',';
load data infile 'token.csv' into table token fields terminated by ',';
load data infile 'Account.csv' into table account fields terminated by ',';


# 有新的交易数据写入后，根据最新的成交价格更新token的最新price
DELIMITER //
CREATE trigger update_token_price
AFTER INSERT on transaction
FOR EACH ROW
BEGIN
    UPDATE token SET token.price=new.price
    WHERE new.token=token.token;
END//
DELIMITER ;

#有新的交易数据写入后更新一个币种的24小时成交量
DELIMITER //
CREATE trigger update_token_volumn
AFTER INSERT on transaction
FOR EACH ROW
BEGIN
    UPDATE token SET
    token.volumn_24h=(
    SELECT sum(price*amount)
    FROM transaction
    WHERE transaction.time_stamp > (date_sub(now(),interval 1 day))
    )
    where token.token = NEW.token;
END//
DELIMITER ;

# 插入新的交易数据后调整用户钱包的position
DELIMITER //
CREATE trigger update_account_position
AFTER INSERT on transaction
FOR EACH ROW
BEGIN
	IF new.side='buy' THEN
		UPDATE account SET account.position=account.position+new.amount;
    ELSE UPDATE account SET account.position=account.position-new.amount;
    END IF;
END//
DELIMITER ;

# 将新的交易数据写入前，检查account的position来判断该交易数据是否有误。
DELIMITER //
CREATE trigger check_position
BEFORE INSERT on transaction
FOR EACH ROW
BEGIN
	DECLARE msg VARCHAR(10);
	IF new.side='sell' AND account.position<new.amount THEN
		SET msg='余额不足，交易信息错误';
    END IF;
END//
DELIMITER ;





