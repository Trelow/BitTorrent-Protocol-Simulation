#include <fstream>
#include <iostream>
#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

// Definim constantele
#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100
#define RESPONSE_SIZE 5

// Tag-urile pentru MPI
#define TRACKER_INIT_TAG 0
#define TRACKER_REQ_TAG 1
#define TRACKER_RES_TAG 2
#define UPLOAD_REQ_TAG 3
#define UPLOAD_RES_TAG 4

// Codurile pentru request-uri
#define SWARM_UPDATE_CODE 0
#define SWARM_REQ_CODE 1
#define DOWNLOAD_SEGMENT_CODE 2
#define FILE_DOWNLOAD_FINISHED_CODE 3
#define ALL_FINISHED_CODE 4

// Tipuri de clienti
enum client_type { LEECHER, PEER, SEED };

using namespace std;

// Structura ce contine informatii despre un fisier
typedef struct {
  int num_chunks;
  char name[MAX_FILENAME];
  char chunks[MAX_CHUNKS][HASH_SIZE + 1];
} file_struct;

// Structura ce contine datele unui client
typedef struct {
  int owned_files_num;
  file_struct owned_files[MAX_FILES];
  int needed_files_num;
  char needed_files_names[MAX_FILES][MAX_FILENAME];
} client_data;

// Structura pentru a trimite argumente la thread-uri
typedef struct {
  int rank;
  client_data *data;
} thread_args;

// Functia pentru citirea datelor din fisierul clientului
void read_data(int rank, client_data *data) {
  // Deschidem fisierul
  string filename = "in" + to_string(rank) + ".txt";
  ifstream file(filename);

  if (!file.is_open())
    exit(-1);

  // Numarul de fisiere detinute
  file >> data->owned_files_num;
  // Citim nule fisierelor detinute, si segmentele acestora (hash-urile)
  for (int i = 0; i < data->owned_files_num; i++) {
    file >> data->owned_files[i].name;
    file >> data->owned_files[i].num_chunks;
    for (int j = 0; j < data->owned_files[i].num_chunks; j++)
      file >> data->owned_files[i].chunks[j];
  }
  // Numarul de fisiere dorite
  file >> data->needed_files_num;
  for (int i = 0; i < data->needed_files_num; i++)
    file >> data->needed_files_names[i];

  file.close();
}

// Functia pentru a gasi indexul fisierului dupa nume
int find_file_index(const char *filename, file_struct *files, int files_count) {
  for (int i = 0; i < files_count; i++)
    if (strcmp(files[i].name, filename) == 0)
      return i;
  return -1;
}

// Functia pentru a primi informatii despre un fisier de la tracker
void get_swarm_info(const char *filename, vector<int> &swarm_peers,
                    bool request_hashes) {
  // Daca avem nevoie sa actualizam lista de peers/seeds, trimitem un request
  // corespunzator
  int request_code = SWARM_UPDATE_CODE;
  // Daca e primul request, cerem de asemenea si hash-urile segmentelor
  if (request_hashes) {
    request_code = SWARM_REQ_CODE;
  }

  MPI_Send(&request_code, 1, MPI_INT, TRACKER_RANK, TRACKER_REQ_TAG,
           MPI_COMM_WORLD);
  MPI_Send(filename, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TRACKER_REQ_TAG,
           MPI_COMM_WORLD);
  // Primim nr de peers
  int peers_num = 0;
  MPI_Recv(&peers_num, 1, MPI_INT, TRACKER_RANK, TRACKER_RES_TAG,
           MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  // Primim indecsii acestora
  swarm_peers.clear();
  swarm_peers.resize(peers_num);
  for (int i = 0; i < peers_num; i++) {
    MPI_Recv(&swarm_peers[i], 1, MPI_INT, TRACKER_RANK, TRACKER_RES_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }
}

// Functia pentru a procesa un swarm request
void process_swarm_request(int sender, int request_code,
                           file_struct *swarm_files, int swarm_files_count,
                           vector<pair<int, client_type>> *swarm_clients,
                           int *leechers_num) {
  char filename[MAX_FILENAME];
  // Primim numele
  MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, sender, TRACKER_REQ_TAG,
           MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  // Cautam fisierul in swarm
  int file_index = find_file_index(filename, swarm_files, swarm_files_count);

  // Trimitem numarul de peers/seeds
  int peers_num = swarm_clients[file_index].size() - leechers_num[file_index];
  MPI_Send(&peers_num, 1, MPI_INT, sender, TRACKER_RES_TAG, MPI_COMM_WORLD);
  // Trimitem indecsii acestora
  for (size_t i = 0; i < swarm_clients[file_index].size(); i++) {
    // Daca e leecher, nu il trimitem
    if (swarm_clients[file_index][i].second != LEECHER) {
      MPI_Send(&swarm_clients[file_index][i].first, 1, MPI_INT, sender,
               TRACKER_RES_TAG, MPI_COMM_WORLD);
      peers_num--;
      if (peers_num == 0)
        break;
    }
  }

  // Dace e cerere de actualizare, schimbam leecher-ul in peer
  // pentru ca erau descarcate cel putin 10 segmente
  if (request_code == SWARM_UPDATE_CODE) {
    // Gasim leecher-ul in lista si il marcam ca peer
    for (size_t i = 0; i < swarm_clients[file_index].size(); i++) {
      if (swarm_clients[file_index][i].first == sender &&
          swarm_clients[file_index][i].second == LEECHER) {
        swarm_clients[file_index][i].second = PEER;
        leechers_num[file_index]--;
        break;
      }
    }
  }

  // Daca e o cerere pentru fisier, trimitem si hash-urile segmentelor
  if (request_code == SWARM_REQ_CODE) {
    MPI_Send(&swarm_files[file_index].num_chunks, 1, MPI_INT, sender,
             TRACKER_RES_TAG, MPI_COMM_WORLD);
    for (int i = 0; i < swarm_files[file_index].num_chunks; i++)
      MPI_Send(swarm_files[file_index].chunks[i], HASH_SIZE + 1, MPI_CHAR,
               sender, TRACKER_RES_TAG, MPI_COMM_WORLD);
    // Adaugam leecher-ul in lista
    swarm_clients[file_index].push_back({sender, LEECHER});
    leechers_num[file_index]++;
  }
}

// Functia pentru procesarea mesajului de finalizare a descarcarii
// unui fisier
void file_download_finished(int sender, file_struct *swarm_files,
                            int swarm_files_count,
                            vector<pair<int, client_type>> *swarm_clients) {
  char filename[MAX_FILENAME];
  MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, sender, TRACKER_REQ_TAG,
           MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  // Cautam fisierul
  int file_index = find_file_index(filename, swarm_files, swarm_files_count);
  // Cautam peer-ul in lista si il marcam ca seed
  for (size_t i = 0; i < swarm_clients[file_index].size(); i++) {
    if (swarm_clients[file_index][i].first == sender) {
      swarm_clients[file_index][i].second = SEED;
      break;
    }
  }
}

// Functia pentru procesarea mesajului de finalizarea tuturor descarcarilor
// a unui client
void all_files_downloaded(int sender, vector<bool> &clients_finished,
                          bool &all_clients_finished) {
  clients_finished[sender - 1] = true;
  // Verificam daca toti clientii au terminat, daca da, iesim din loop
  all_clients_finished = true;
  for (size_t i = 0; i < clients_finished.size(); i++) {
    if (!clients_finished[i]) {
      all_clients_finished = false;
      break;
    }
  }
}

// Functia care trimite file-urile detinute de un peer catre tracker
void send_files_to_tracker(int rank, client_data *data) {
  // Nr de fisiere detinute
  MPI_Send(&data->owned_files_num, 1, MPI_INT, TRACKER_RANK, TRACKER_INIT_TAG,
           MPI_COMM_WORLD);
  // Trimitem numele fisierelor, numarul de segmente si hash-urile acestora
  for (int i = 0; i < data->owned_files_num; i++) {
    MPI_Send(data->owned_files[i].name, MAX_FILENAME, MPI_CHAR, TRACKER_RANK,
             TRACKER_INIT_TAG, MPI_COMM_WORLD);
    MPI_Send(&data->owned_files[i].num_chunks, 1, MPI_INT, TRACKER_RANK,
             TRACKER_INIT_TAG, MPI_COMM_WORLD);
    for (int j = 0; j < data->owned_files[i].num_chunks; j++)
      MPI_Send(data->owned_files[i].chunks[j], HASH_SIZE + 1, MPI_CHAR,
               TRACKER_RANK, TRACKER_INIT_TAG, MPI_COMM_WORLD);
  }

  char ack[RESPONSE_SIZE];
  // Primim ACK de la tracker
  MPI_Recv(ack, RESPONSE_SIZE, MPI_CHAR, TRACKER_RANK, TRACKER_INIT_TAG,
           MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  if (strcmp(ack, "OK") != 0)
    cerr << "Failed to send files to tracker\n";
}

// Functia care salveaza hash-urile intr-un fisier
void save_file(const char chunks[][HASH_SIZE + 1], int num_chunks,
               const std::string &name, int rank) {
  // Cream numele fisierului
  std::string filename = "client" + std::to_string(rank) + "_" + name;
  // Deschidem fisierul
  std::ofstream file(filename);

  if (file.is_open()) {
    // Scriem hash-urile segmentelor
    for (int i = 0; i < num_chunks; i++)
      file << chunks[i] << std::endl;
    file.close();
  } else {
    cerr << "Failed to open " << filename << endl;
  }
}

// Functia pentru descarcare unui segment
bool download_segment(int seg, file_struct &file, vector<int> &swarm_peers,
                      client_data *data, int file_index, int &downloaded_chunks,
                      bool *chunks_received, int rank) {
  int tried_peers = 0;
  bool no_peer_found = false;
  while (!chunks_received[seg]) {
    // Daca s-au incercat toti peer-ii, iesim
    if (tried_peers == (int)swarm_peers.size()) {
      cerr << "Nu a fost gasit niciun peer pentru segmentul " << seg
           << " al fisierului " << file.name << "\n";
      no_peer_found = true;
      break;
    }
    // Conform algoritmului, round-robin, fiecare segment se descarca de
    // la un peer diferit, astfel incercam sa distribuim cat mai bine
    // segmentele
    int chosen = (downloaded_chunks % swarm_peers.size()) + tried_peers;
    if (chosen >= (int)swarm_peers.size())
      chosen -= swarm_peers.size();
    // Alegem peer-ul
    int chosen_peer = swarm_peers[chosen];
    // Trimitem request-ul pentru segment
    int request_code = DOWNLOAD_SEGMENT_CODE;
    MPI_Send(&request_code, 1, MPI_INT, chosen_peer, UPLOAD_REQ_TAG,
             MPI_COMM_WORLD);
    MPI_Send(file.name, MAX_FILENAME, MPI_CHAR, chosen_peer, UPLOAD_REQ_TAG,
             MPI_COMM_WORLD);
    MPI_Send(&seg, 1, MPI_INT, chosen_peer, UPLOAD_REQ_TAG, MPI_COMM_WORLD);
    // Primim raspunsul
    char res[RESPONSE_SIZE];
    MPI_Recv(res, RESPONSE_SIZE, MPI_CHAR, chosen_peer, UPLOAD_RES_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    // Daca e OK, verificam hash-ul
    if (strcmp(res, "OK") == 0) {
      char hash[HASH_SIZE + 1];
      // Primim hash-ul
      MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, chosen_peer, UPLOAD_RES_TAG,
               MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      if (strcmp(file.chunks[seg], hash) == 0) {
        // Daca hash-ul este corect, marcam segmentul ca descarcat
        chunks_received[seg] = true;
        downloaded_chunks++;
        // Marim numarul de segmente detinute
        data->owned_files[data->owned_files_num - 1].num_chunks++;
        // Copiem hash-ul
        strcpy(data->owned_files[file_index].chunks[seg], hash);
      } else {
        // Daca hash-ul nu este corect, incercam alt peer
        tried_peers++;
      }
    } else if (strcmp(res, "NACK") == 0) {
      // Daca peer/seed-ul nu detine segmentul, incercam altul
      tried_peers++;
    }
  }
  return no_peer_found;
}

// Functia pentru actualizarea listei de peers/seeds
void update_swarm_list(int rank, file_struct &file, vector<int> &swarm_peers) {
  // Primim lista de peers/seeds
  get_swarm_info(file.name, swarm_peers, false);
  // Stergem peer-ul curent din lista
  for (size_t i = 0; i < swarm_peers.size(); i++) {
    if (swarm_peers[i] == rank) {
      swarm_peers.erase(swarm_peers.begin() + i);
      break;
    }
  }
}

// Functia pentru descarcarea segmentelor de la un peers/seeds
void *download_thread_func(void *arg) {
  // Extragem argumentele
  thread_args *args = (thread_args *)arg;
  client_data *data = args->data;
  int rank = args->rank;
  int request_code;

  // Incercam sa descarcam toate fisierele dorite
  for (int i = 0; i < data->needed_files_num; i++) {
    file_struct file;
    // Lista de peers/seeds
    vector<int> swarm_peers;
    // Chunk-urile descarcate
    bool chunks_received[MAX_CHUNKS] = {false};
    // Marim numarul de fisiere detinute
    data->owned_files_num++;
    // Indexul fisierului curent
    int file_index = data->owned_files_num - 1;
    // Numarul de segmente descarcate
    int downloaded_chunks = 0;

    strcpy(file.name, data->needed_files_names[i]);
    strcpy(data->owned_files[file_index].name, file.name);
    // Primim informatii despre swarm, inclusiv hash-urile segmentelor
    // Cu ajutorul lor se va face verificarea corectitudinii segmentelor
    get_swarm_info(file.name, swarm_peers, true);
    MPI_Recv(&file.num_chunks, 1, MPI_INT, TRACKER_RANK, TRACKER_RES_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (int j = 0; j < file.num_chunks; j++)
      MPI_Recv(file.chunks[j], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK,
               TRACKER_RES_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // Cream un vector cu segmentele ce trebuie descarcate
    vector<int> segments_to_download;
    for (int j = 0; j < file.num_chunks; j++)
      segments_to_download.push_back(j);

    bool no_peer_found = false;
    // Cat timp nu am descarcat toate segmentele, incercam sa le descarcam
    while (downloaded_chunks < file.num_chunks) {
      // Daca nu s a gasit niciun peer, pentru segmentul curent, iesim
      if (no_peer_found) {
        break;
      }
      // Alegem un segment random, astfel daca peer-urile detin segmente
      // diferite, sunt sanse mai mari ca segmentele sa fie descarcate
      // de la peer-uri diferite, crescand astfel eficienta algoritmului
      int seg = segments_to_download[rand() % segments_to_download.size()];
      // Stergem segmentul dupa ce l-am ales
      for (size_t i = 0; i < segments_to_download.size(); i++) {
        if (segments_to_download[i] == seg) {
          segments_to_download.erase(segments_to_download.begin() + i);
          break;
        }
      }
      // Incercam sa descarcam segmentul de la un peer, folosim algoritmul
      // round-robin
      no_peer_found =
          download_segment(seg, file, swarm_peers, data, file_index,
                           downloaded_chunks, chunks_received, rank);
      // La fiecare 10 segmente descarcate, actualizam lista de peers/seeds
      if (downloaded_chunks % 10 == 0)
        update_swarm_list(rank, file, swarm_peers);
    }
    // Dupa ce am toate segmentele, faceam request pentru a marca
    // finalizarea descarcarii a fisierului
    int requestCode = FILE_DOWNLOAD_FINISHED_CODE;
    MPI_Send(&requestCode, 1, MPI_INT, TRACKER_RANK, TRACKER_REQ_TAG,
             MPI_COMM_WORLD);
    MPI_Send(file.name, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TRACKER_REQ_TAG,
             MPI_COMM_WORLD);
    // Salvam fisierul
    save_file(data->owned_files[file_index].chunks, file.num_chunks, file.name,
              rank);
  }

  // Dupa ce am descarcat toate fisierele, trimitem un request pentru a marca
  // finalizarea descarcarii
  request_code = ALL_FINISHED_CODE;
  MPI_Send(&request_code, 1, MPI_INT, TRACKER_RANK, TRACKER_REQ_TAG,
           MPI_COMM_WORLD);

  return NULL;
}

// Functia pentru a trimite segmentele cerute de un peer/leecher
void *upload_thread_func(void *arg) {
  // Extragem argumentele
  thread_args *args = (thread_args *)arg;
  client_data *data = args->data;

  bool running = true;
  // Cat timp n-am primit mesaj de terminare de la tracker
  while (running) {
    int request_code;
    MPI_Status status;
    // Asteptam request-uri
    MPI_Recv(&request_code, 1, MPI_INT, MPI_ANY_SOURCE, UPLOAD_REQ_TAG,
             MPI_COMM_WORLD, &status);
    // Extragem sursa
    int sender = status.MPI_SOURCE;

    // Daca primim request pentru un segment
    if (request_code == DOWNLOAD_SEGMENT_CODE) {
      int seg_index;
      char filename[MAX_FILENAME];
      bool have_segment = false;
      // Primim numele fisierului si indexul segmentului
      MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, sender, UPLOAD_REQ_TAG,
               MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Recv(&seg_index, 1, MPI_INT, sender, UPLOAD_REQ_TAG, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
      // Cautam mai intai fisierul
      for (int i = 0; i < data->owned_files_num; i++) {
        if (strcmp(data->owned_files[i].name, filename) == 0) {
          // Daca avem segmentul, trimitem OK si hash-ul
          if (data->owned_files[i].chunks[seg_index][0] != '\0') {
            char res[] = "OK";
            MPI_Send(res, (int)strlen(res) + 1, MPI_CHAR, sender,
                     UPLOAD_RES_TAG, MPI_COMM_WORLD);
            MPI_Send(data->owned_files[i].chunks[seg_index], HASH_SIZE + 1,
                     MPI_CHAR, sender, UPLOAD_RES_TAG, MPI_COMM_WORLD);
            have_segment = true;
            break;
          }
        }
      }

      // Daca nu am gasit segmentul, trimitem NACK
      if (!have_segment) {
        char res[] = "NACK";
        MPI_Send(res, (int)strlen(res) + 1, MPI_CHAR, sender, UPLOAD_RES_TAG,
                 MPI_COMM_WORLD);
      }
    } else if (request_code == ALL_FINISHED_CODE) {
      // Daca primim mesaj de terminare, iesim
      running = false;
    }
  }
  return NULL;
}

// Functia pentru a initializa datele tracker-ului
void initialize_tracker_data(int num_tasks, file_struct *swarm_files,
                             int &swarm_files_count,
                             vector<pair<int, client_type>> *swarm_peers) {
  // Primim fisierele detinute de fiecare client
  for (int rank = 1; rank < num_tasks; rank++) {
    int owned_files_num = 0;
    MPI_Recv(&owned_files_num, 1, MPI_INT, rank, TRACKER_INIT_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    // Primim numele fisierelor, numarul de segmente si hash-urile lor
    for (int i = 0; i < owned_files_num; i++) {
      char filename[MAX_FILENAME];
      MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, rank, TRACKER_INIT_TAG,
               MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      int num_chunks = 0;
      MPI_Recv(&num_chunks, 1, MPI_INT, rank, TRACKER_INIT_TAG, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
      swarm_files[swarm_files_count].num_chunks = num_chunks;

      // Cautam indexul fisierului
      int file_index =
          find_file_index(filename, swarm_files, swarm_files_count);
      // Daca nu exista, il adaugam
      if (file_index == -1) {
        file_index = swarm_files_count;
        strcpy(swarm_files[file_index].name, filename);
        swarm_files_count++;
      }
      // Primim hash-urile segmentelor
      for (int j = 0; j < num_chunks; j++) {
        char hashes[HASH_SIZE + 1];
        MPI_Recv(hashes, HASH_SIZE + 1, MPI_CHAR, rank, TRACKER_INIT_TAG,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        strcpy(swarm_files[file_index].chunks[j], hashes);
      }
      // Adaugam clientul ca seed
      swarm_peers[file_index].push_back({rank, SEED});
    }
  }
  // Trimitem ACK-uri la toti clientii, ca datele au fost initializate
  for (int rank = 1; rank < num_tasks; rank++) {
    char ack[3] = "OK";
    MPI_Send(ack, 3, MPI_CHAR, rank, TRACKER_INIT_TAG, MPI_COMM_WORLD);
  }
}

// Functia de tracker
void tracker(int num_tasks, int rank) {
  // Numarul de fisiere in swarm
  int swarm_files_count = 0;
  // Lista de fisiere in swarm
  file_struct swarm_files[MAX_FILES];
  // Lista de peers/seeds/leechers pentru fiecare fisier
  // Daca bool-ul este true, inseamna ca peer-ul este seed
  vector<pair<int, client_type>> swarm_clients[MAX_FILES];
  int leechers_num[MAX_FILES] = {0};
  // Lista de clienti care au terminat de descarcat
  vector<bool> clients_finished(num_tasks - 1, false);

  // Initializam datele tracker-ului
  initialize_tracker_data(num_tasks, swarm_files, swarm_files_count,
                          swarm_clients);

  bool all_clients_finished = false;
  // Cat timp nu au terminat toti clientii
  int request_code;
  while (!all_clients_finished) {
    MPI_Status status;
    // Primim request-uri de la clienti
    MPI_Recv(&request_code, 1, MPI_INT, MPI_ANY_SOURCE, TRACKER_REQ_TAG,
             MPI_COMM_WORLD, &status);
    // Extragem sursa
    int sender = status.MPI_SOURCE;
    // Daca primim request pentru swarm-ul unui fisier
    if (request_code == SWARM_UPDATE_CODE || request_code == SWARM_REQ_CODE) {
      process_swarm_request(sender, request_code, swarm_files,
                            swarm_files_count, swarm_clients, leechers_num);
    } else if (request_code == FILE_DOWNLOAD_FINISHED_CODE) {
      // Daca un client a terminat de descarcat un fisier, marcam peer-ul ca
      // seed
      file_download_finished(sender, swarm_files, swarm_files_count,
                             swarm_clients);
    } else if (request_code == ALL_FINISHED_CODE) {
      // Daca toate clientul a descarcat toate fisierele, marcam clientul ca
      // terminat
      all_files_downloaded(sender, clients_finished, all_clients_finished);
    }
  }
  // Dupa ce toti clientii au terminat, inchidem toate thread-urile de upload
  for (int rank = 1; rank < num_tasks; rank++) {
    request_code = ALL_FINISHED_CODE;
    MPI_Send(&request_code, 1, MPI_INT, rank, UPLOAD_REQ_TAG, MPI_COMM_WORLD);
  }
}

// Functia de rularea a unui peer
void peer(int num_tasks, int rank) {
  pthread_t download_thread;
  pthread_t upload_thread;
  void *status;
  int r;
  // Initializam datele clientului
  client_data data;
  data.owned_files_num = 0;
  data.needed_files_num = 0;
  for (int i = 0; i < MAX_FILES; i++)
    data.owned_files[i].num_chunks = 0;
  for (int i = 0; i < MAX_FILES; i++)
    for (int j = 0; j < MAX_CHUNKS; j++)
      data.owned_files[i].chunks[j][0] = '\0';

  // Citim datele din fisier
  read_data(rank, &data);
  // Trimitem fisierele detinute de client catre tracker
  send_files_to_tracker(rank, &data);
  // Initializam argumentele pentru thread-uri
  thread_args threadArgs;
  threadArgs.rank = rank;
  threadArgs.data = &data;
  // Cream thread-urile pentru download si upload
  r = pthread_create(&download_thread, NULL, download_thread_func,
                     (void *)&threadArgs);
  if (r) {
    printf("Eroare la crearea thread-ului de download\n");
    exit(-1);
  }

  r = pthread_create(&upload_thread, NULL, upload_thread_func,
                     (void *)&threadArgs);
  if (r) {
    printf("Eroare la crearea thread-ului de upload\n");
    exit(-1);
  }

  r = pthread_join(download_thread, &status);
  if (r) {
    printf("Eroare la asteptarea thread-ului de download\n");
    exit(-1);
  }

  r = pthread_join(upload_thread, &status);
  if (r) {
    printf("Eroare la asteptarea thread-ului de upload\n");
    exit(-1);
  }
}

int main(int argc, char *argv[]) {
  int num_tasks, rank;
  int provided;

  // Initializam MPI
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  if (provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
    exit(-1);
  }
  MPI_Comm_size(MPI_COMM_WORLD, &num_tasks);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  // Initializam seed-ul de random pentru fiecare proces
  srand(time(NULL) + rank);

  // Pornim tracker-ul si peer-ii
  if (rank == TRACKER_RANK) {
    tracker(num_tasks, rank);
  } else {
    peer(num_tasks, rank);
  }

  // Inchidem MPI
  MPI_Finalize();
}
