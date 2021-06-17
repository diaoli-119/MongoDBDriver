#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "stdafx.h"
#include "public.h"
#include "dataStructure.h"
#include "cJSON/cJSON.h"

//static const char *uriLocal_str = MONGODBSERVER_LOCAL;
//static const char *uriAws_str = MONGODBSERVER_AWS;
static char *respStr;
static mongoc_client_t *clientLocal;
static mongoc_client_t *clientAws;
static mongoc_collection_t *collectionLocal;
static mongoc_collection_t *collectionAws;
static bson_error_t error;
static mongoc_uri_t *uriLocal;
static mongoc_uri_t *uriAws;
static mongoc_database_t *database;
static bson_oid_t oid;
static bson_t *command;
static bson_t *doc = NULL;
static bson_t *update = NULL;
static bson_t *query = NULL;
static bson_t reply;
static long int fileLen = 0;
static long int count = 1;

float byteToFloat(unsigned char* byteArry) { return *((float*)byteArry); }
int byteToInt(unsigned char* byteArry) { return *((int*)byteArry); }

int hexStrToInt(char s[])
{
	int i = 0;
	int n = 0;
	if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) i = 2;
	else i = 0;

	for (; (s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z'); ++i)
	{
		if (tolower(s[i]) > '9') n = 16 * n + (10 + tolower(s[i]) - 'a');
		else n = 16 * n + (tolower(s[i]) - '0');\
	}
	return n;
}

time_t StringToDatetime(char timeStr[])
{
	struct tm tmIns;
	uint32_t year, month, day, hour, min, sec;
	sscanf(timeStr, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &min, &sec);
	tmIns.tm_year = year - 1900;	//year starts from 1900 in struct tm
	tmIns.tm_mon = month - 1;	//month: 0 - 11
	tmIns.tm_mday = day;
	tmIns.tm_hour = hour;
	tmIns.tm_min = min;
	tmIns.tm_sec = sec;
	tmIns.tm_isdst = 0;			//un-saving-time
	return mktime(&tmIns);
}

bool convertMeshInfo(char meshInfo[], uint32_t type, char watchName[], uint16_t nodeAddr[], time_t *createdTime, float *ptrTemp, float *ptrHumi, float *ptrXPos, 
					float *ptrYPos, double *ptrbTemp, char *chekStat, char batAV[], char batBV[], char testTime[], char crc16[])
{
	uint16_t i = 0;
	uint16_t n = 0;

	/*Obtain created time string*/
	char createdTimeStr[TIME_STR_LEN] = {0};
	while ('\t' != meshInfo[i] && i < CREATED_TIME_BUF)
	{
		createdTimeStr[i] = meshInfo[i];
		i++;
	}
	*createdTime = StringToDatetime(createdTimeStr) + 3600 * 12;	//plus 12 hours
	
	/*jump over identifier "?"*/
	while ('?' != meshInfo[i++]);

	/*Get node addr*/
	memcpy(nodeAddr, (const char *)&meshInfo[i++], 2);

	/*jump over identifier "!"*/
	while ('!' != meshInfo[i++]);

	/*Get data length*/
	//uint16_t dataLen = meshInfo[i++];
	
	/*jump over dataLen byte*/
	i++;
	
	/*jump over "type"*/
	i += HEAD_LEN + TYPE_LEN;

	if ( MSG_TYPE_TH == type )		//SHT30 Temp&Humi Sensor msg. T:0x54, H:0x48, type:0x5448
	{
		/*Convert temperature*/
		char tempStr[ENV_TEMP_LEN] = { 0 };
		for (; i < i + ENV_TEMP_LEN && n < ENV_TEMP_LEN; i++, n++) tempStr[n] = meshInfo[i];
		*ptrTemp = byteToFloat((unsigned char *)tempStr);
		printf("%0.2f\n", *ptrTemp);

		/*Convert humidity*/
		char humiStr[ENV_HUMI_LEN] = { 0 };
		for (n = 0; i < i + ENV_HUMI_LEN && n < ENV_HUMI_LEN; i++, n++) humiStr[n] = meshInfo[i];
		*ptrHumi = byteToFloat((unsigned char *)humiStr);
		printf("%0.2f\n", *ptrHumi);
		return true;
	}
	else if ( MSG_TYPE_PO == type )	//Position msg. P:0x50, O:0x4F, type: 0x504F
	{
		/*Convert position*/
		char posStrX[X_POS_LEN] = { 0 };
		char posStrY[Y_POS_LEN] = { 0 };

		/*In case position data is empty*/
		if ('\n' == meshInfo[i]) return false;
		for (; i < i + X_POS_LEN && n < X_POS_LEN; i++, n++) posStrX[n] = meshInfo[i];
		for (n = 0; i < i + Y_POS_LEN && n < Y_POS_LEN; i++, n++) posStrY[n] = meshInfo[i];
		*ptrXPos = byteToFloat((unsigned char *)posStrX);
		*ptrYPos = byteToFloat((unsigned char *)posStrY);
		printf("%0.2f\n", *ptrXPos);
		printf("%0.2f\n", *ptrYPos);
		return true;
	}
	else if ( MSG_TYPE_PI == type )	//PIR Sensor msg. P:0x50, I:0x4F, type: 0x5049
	{	
		return true;
	}
	else if ( MSG_TYPE_BE == type )	//body temperature from watch
	{
		uint16_t nameLen = meshInfo[i++] - 1;	//name does not include 0x09, so the actual nameLen is meshInfo[i] - 1

		/*jump over 0x09 and watch name*/
		i++;

		/*Get watch name*/
		for (; i < i + nameLen && n < nameLen; i++, n++) watchName[n] = meshInfo[i];

		/*jump over 0x10, 0x16, uuid: 0x03, 0x18*/
		i += 4;

		/*Convert body temperature*/
		char bIntTempStr[4] = { 0 };
		char bDecTempStr[4] = { 0 }; 
		sprintf(bIntTempStr, "%d", meshInfo[i++]);
		sprintf(bDecTempStr, "%d", meshInfo[i]);
		char tempStr[16] = { 0 };
		strncpy(tempStr, bIntTempStr, strlen(bIntTempStr));
		strncat(tempStr, ".", 2);
		strncat(tempStr, bDecTempStr, strlen(bDecTempStr));
		*ptrbTemp = atof(tempStr);

		return true;
	}
	else if ( MSG_TYPE_R1 == type )
	{
		*chekStat = meshInfo[i++];
		batAV[0] = meshInfo[i++];
		batAV[1] = meshInfo[i++];
		batBV[0] = meshInfo[i++];
		batBV[1] = meshInfo[i++];
		testTime[0] = meshInfo[i++];
		testTime[1] = meshInfo[i++];
		crc16[0] = meshInfo[i++];
		crc16[1] = meshInfo[i++];
		return true;
	}
	return false;
}

void genRandomStr(char *s, const int len)
{
	srand((count++) * time(0));
	const char rawStr[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
	for (int i = 0; i < len; ++i)
	{
		s[i] = rawStr[rand() % (sizeof(rawStr) - 1)];
	}
	s[len - 1] = 0;
}

void genRandomNum(int *num, const int len)
{
	for (int c = 0; c < len; c++)
	{
		*num = rand() % 100 + 1;
	}
}

void uploadDataToMongoServer(char header[], uint32_t type, char watchName[], uint16_t *nodeAddr, time_t createdTime, float *ptrTemp, float *ptrHumi, float *ptrXPos, 
							 float *ptrYPos, double *ptrbTemp, char chekStat[], char batAV[], char batBV[], char testTime[], char crc16[] )
{
	time_t timer;
	time(&timer);
	long long int llUpdatedTime = ((long long int)timer) * 1000;
	long long int llCreatedTime = ((long long int)createdTime) * 1000;

//	char updatedTime[20];
//	time_t now = time(NULL);
//	strftime(updatedTime, 20, "%Y-%m-%d %H:%M:%S", localtime(&now));
	
	/*Generate userId*/
	char userId[24] = { 0 };
	genRandomStr(userId, 24);

	bson_oid_init(&oid, NULL);
	switch (type)
	{
		case MSG_TYPE_PO:
		{
			/*Get Position*/
			collectionAws = mongoc_client_get_collection(clientAws, "parse", DBTAB_POSITION);
			doc = BCON_NEW(
				  "createdAt", BCON_DATE_TIME(llCreatedTime),
				  "updatedAt", BCON_DATE_TIME(llUpdatedTime),
					 "header", BCON_UTF8(header),
					   "type", BCON_UTF8("MSG_TYPE_PO"),
				   "nodeAddr", BCON_INT32(*nodeAddr),
					 "userId", BCON_UTF8(userId),
						  "x", BCON_DOUBLE(*ptrXPos),
						  "y", BCON_DOUBLE(*ptrYPos)
				);
			break;
		}
		case MSG_TYPE_TH:
		{
			/*Get SHT30 Temo&Humi Sensor msg*/
			collectionAws = mongoc_client_get_collection(clientAws, "parse", DBTAB_TEMPHUMI);
			doc = BCON_NEW(
				  "createdAt", BCON_DATE_TIME(llCreatedTime),
				  "updatedAt", BCON_DATE_TIME(llUpdatedTime),
					 "header", BCON_UTF8(header),
					   "type", BCON_UTF8("MSG_TYPE_TH"),
				   "nodeAddr", BCON_INT32(*nodeAddr),
					   "temp", BCON_DOUBLE(*ptrTemp),
					   "humi", BCON_DOUBLE(*ptrHumi)
				);
			break;
		}
		case MSG_TYPE_BE:
		{
			/*Get body temperature from watch*/
			collectionAws = mongoc_client_get_collection(clientAws, "parse", DBTAB_BODYTEMP);
			doc = BCON_NEW(
				  "createdAt", BCON_DATE_TIME(llCreatedTime),
				  "updatedAt", BCON_DATE_TIME(llUpdatedTime),
					 "header", BCON_UTF8(header),
					   "type", BCON_UTF8("MSG_TYPE_BE"),
				  "watchName", BCON_UTF8(watchName),
				   "nodeAddr", BCON_INT32(*nodeAddr),
					   "temp", BCON_DOUBLE(*ptrbTemp),
					"macAddr", BCON_UTF8(""),
					"battery", BCON_UTF8(""),
					   "rssi", BCON_UTF8("")
			);
			break;
		}
		case MSG_TYPE_R1:
		{
			/*Update self_check information*/
			collectionAws = mongoc_client_get_collection(clientAws, "parse", DBTAB_SELFCHEK);
			doc = BCON_NEW(
				  "createdAt", BCON_DATE_TIME(llCreatedTime),
				  "updatedAt", BCON_DATE_TIME(llUpdatedTime),
					 "header", BCON_UTF8(header),
					   "type", BCON_UTF8("MSG_TYPE_BE"),
				   "nodeAddr", BCON_INT32(*nodeAddr),
					 "status", BCON_UTF8(chekStat),
					  "batAV", BCON_UTF8(batAV),
					  "batBV", BCON_UTF8(batBV),
				   "testTime", BCON_UTF8(testTime),
					  "crc16", BCON_UTF8(crc16)
			);
			break;
		}
		case MSG_TYPE_R2:
		case MSG_TYPE_PI:
		{
			/*Get PIR*/
			collectionAws = mongoc_client_get_collection(clientAws, "parse", DBTAB_PIR);
			doc = BCON_NEW(
				  "createdAt", BCON_DATE_TIME(llCreatedTime),
				  "updatedAt", BCON_DATE_TIME(llUpdatedTime),
					 "header", BCON_UTF8(header),
					   "type", BCON_UTF8("MSG_TYPE_PI"),
				   "nodeAddr", BCON_INT32(*nodeAddr)
			);
			break;			
		}
		case MSG_TYPE_SD:
		case MSG_TYPE_Alert: 
			return;
		default:
			return;
	}
	if (!mongoc_collection_insert_one(collectionAws, doc, NULL, NULL, &error))
	{
		fprintf(stderr, "update AWS server error: %s\n", error.message);
		goto fail;
	}

fail:
	if (doc)
		bson_destroy(doc);
	if (query)
		bson_destroy(query);
	if (update)
		bson_destroy(update);
}

void readFile()
{
	FILE *fp = NULL;
	while (!(fp = fopen(MESHDATA, "rb")));
	
	char buf[BUF_LEN] = { 0 };
	char watchName[WATCHNAME_LEN] = { 0 };
	char chekStat = 0;
	char batAV[BATAVLEN] = {0};
	char batBV[BATBVLEN] = {0};
	char testTime[TESTTIMELEN] = {0};
	char crc16[CRC16LEN] = {0};
	float temp = 0;
	float humi = 0;
	float xPos = 0;
	float yPos = 0;
	double bTemp = 0;
	time_t createdTime = 0;
	uint32_t type = 0;
	uint16_t nodeAddr = 0;
	uint16_t pos = 0;
	uint16_t i = 0;
	uint16_t item = 0;
	
	/*Set fp to current position*/
	fseek(fp, fileLen, SEEK_SET);
	
	while (!feof(fp))
	{
		do
		{
			buf[i] = fgetc(fp);
			if (buf[i] == 0x0a && buf[i - 1] == 0x09 && buf[i - 2] == 0x0a) break;	//last three bytes in every line
			i++;
		} while (i < BUF_LEN);

		while (pos < BUF_LEN)
		{
			if ('D' == buf[pos++] && 'N' == buf[pos++])
			{
				strncpy((char *)&type, &buf[pos + 1], 1);
				strncat((char *)&type, &buf[pos], 1);
				if (convertMeshInfo(buf, type, watchName, &nodeAddr, &createdTime, &temp, &humi, &xPos, &yPos, &bTemp, &chekStat, batAV, batBV, testTime, crc16))
				{
					uploadDataToMongoServer(RECORD_HEADER, type, watchName, &nodeAddr, createdTime, &temp, &humi, &xPos, &yPos, &bTemp, &chekStat, batAV, batBV, testTime, crc16);
				}
				type = 0;
				i = 0;
				pos = 0;
				item++;
				break;
			}
		}
	}
	
	/*get length of current file*/
	fileLen = ftell(fp);
	
	printf("The number of records is %d\n", item);
	fclose(fp);
	fp = NULL;
}

int connectToMongoServer()
{
	/*
	 * Required to initialize libmongoc's internals
	 */
	mongoc_init();

	 /*
	  * Safely create a MongoDB URI object from the given string
	  */
	uriAws = mongoc_uri_new_with_error(MONGODBSERVER_AWS, &error);
	if (!uriAws)
	{
		fprintf(stderr,
			"failed to parse URI: %s\n"
			"error message:       %s\n",
			MONGODBSERVER_AWS,
			error.message);
		return EXIT_FAILURE;
	}

	/*
	 * Create a new client instance
	 */
	if (!(clientAws = mongoc_client_new_from_uri(uriAws))) return EXIT_FAILURE;

	/*
	 * Register the application name so we can track it in the profile logs
	 * on the server. This can also be done from the URI (see other examples).
	 */
	if (!mongoc_client_set_appname(clientAws, "parse")) return EXIT_FAILURE;

	 /*
	  * Do work. This example pings the database, prints the result as JSON and
	  * performs an insert
	  */
	command = BCON_NEW("ping", BCON_INT32(1));

	if (!mongoc_client_command_simple(clientAws, "admin", command, NULL, &reply, &error))
	{
		fprintf(stderr, "%s\n", error.message);
		return EXIT_FAILURE;
	}
	respStr = bson_as_json(&reply, NULL);
	printf("AWS mongodb: %s\n", respStr);

	/*Upate data to cloud*/
	if (strstr((const char *)respStr, "ok"))
	{
		while(true) 
		{
			readFile();
			sleep(20);
		}
	}

	bson_destroy(&reply);
	bson_destroy(command);
	bson_free(respStr);

	/*
	 * Release our handles and clean up libmongoc
	 */
	mongoc_collection_destroy(collectionLocal);
	mongoc_collection_destroy(collectionAws);
	mongoc_database_destroy(database);
	mongoc_uri_destroy(uriLocal);
	mongoc_client_destroy(clientLocal);
	mongoc_uri_destroy(uriAws);
	mongoc_client_destroy(clientAws);
	mongoc_cleanup();

	return EXIT_SUCCESS;
}