#include <mpi.h>
#include <iostream>
#include <fstream>
#include <omp.h>
#include <ctime>
#include <map>
#include <cstdlib>

using namespace std;

int n = 100;
int neighbor;
int myrank, npes;
int nlocal = 25;

struct M
{
    int num;
    int count;
};
M hashMap[100];
M elmnts[100];
M relmnts[100];

fstream dataFile;
MPI_Status stat;

// Function prototype
void mapper();
void Reducer(M e[]);
void LocalSort(M[], int);
void Merge(M elmnts[], M relmnts[], int n, int flag);
int getNeighbor(int round, int myrank, int npes);
void Print(M h[]);

int main(int argc, char* argv[])
{
    //int thread_count = strtol(argv[1], NULL, 10);

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);

    double beginTime, endTime;
    beginTime = MPI_Wtime();

    // Defining a new MPI Datatype
    MPI_Datatype rowtype;
    int array_of_blocklengths[2] = { 4,4 };	
	MPI_Aint array_of_displacements[2] = { 0,0 };
	array_of_displacements[1] = sizeof(MPI_INT);
	MPI_Datatype array_of_types[2] = { MPI_INT, MPI_INT };
	MPI_Type_create_struct(2, array_of_blocklengths, array_of_displacements, array_of_types, &rowtype);
	MPI_Type_commit(&rowtype);
	MPI_Aint extent = 0;
	MPI_Aint lb = 0;
	MPI_Type_get_extent(MPI_INT, &lb, &extent);

    // p0 reads the file and stores the array
    if (myrank == 0)
    {
        dataFile.open("r.txt", ios::in);
        cout << "\n\n\t\tUsing MapReduce to count numbers\n******************************************************************************\n\n";
        if (dataFile)
            cout << "\nFile opened successfully.\n\n";
        else
            cout << "File open error!\n";
        mapper();  
        dataFile.close();  
        cout << "The array is : \n\n";
        for(int i=0; i<100; i++)
             cout << hashMap[i].num << " " << "[" << hashMap[i].count << "]" << "  " ;
        cout << "\n\n******************************************************************************\n\n";
    } 
    
    // p0 scatters the array
    if (myrank == 0)
        {
            for(int i=0; i<npes; i++)
            MPI_Send (hashMap+i*nlocal, nlocal, rowtype, i, 0, MPI_COMM_WORLD); 
            MPI_Recv (elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD, &stat);
        }   
        else if (myrank == 1)    
                MPI_Recv(elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD, &stat);
        else if (myrank == 2)    
                MPI_Recv(elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD, &stat);
        else if (myrank == 3)    
                MPI_Recv(elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD, &stat);

    // Local sorting
    LocalSort(elmnts, nlocal);

    // Global sorting
    for (int i = 0; i < npes; i++)
    {
        neighbor = getNeighbor(i, myrank, npes);
        if ( neighbor != MPI_PROC_NULL)
        {
            // Using p2p communication to transfer data
            MPI_Send (elmnts, nlocal, rowtype, neighbor, 0, MPI_COMM_WORLD);
            MPI_Recv(relmnts, nlocal, rowtype, neighbor, 0, MPI_COMM_WORLD, &stat);
            Merge(elmnts, relmnts, nlocal, myrank - neighbor);
        }
    }

    // Calculating the number and its counts
    Reducer(elmnts);

    // Gather the results
    if (myrank == 1)    
        MPI_Send(elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD);
    else if (myrank == 2)    
        MPI_Send(elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD);
    else if (myrank == 3)    
        MPI_Send(elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD);
    else if (myrank == 0)
    {
        MPI_Send(elmnts, nlocal, rowtype, 0, 0, MPI_COMM_WORLD);
        for(int i=0; i<npes; i++)
            MPI_Recv(hashMap+i*nlocal, nlocal, rowtype, i, 0, MPI_COMM_WORLD, &stat);      
    }

    endTime = MPI_Wtime();

    
    // Printing the final results
    if(myrank == 0)
    {
        Print(hashMap); 
        cout << "\nSpent time = " << endTime - beginTime <<  " second\n\n\n"; 
        cout << "\n\n******************************************************************************\n\n";
    }

    MPI_Finalize();
    
    return 0;
}

// Functions
void mapper()
{
    int i = 0;
    // Using the OpenMP to optimize the data collection
    #pragma omp parallel for num_threads(5) private(i)
    for (i = 0; i < n; i++)
    {
            dataFile >> hashMap[i].num;
            hashMap[i].count = 1; 
    } 

}

void Reducer(M e[])
{
    int k = 0;
    do
    {
        if(elmnts[k].num == elmnts[k+1].num)
        {
            elmnts[k+1].count += elmnts[k].count;
            elmnts[k].count = 0;
            k++;
        }
        else
        {
            k++;
        }
        
    } while (k != nlocal);
}

void LocalSort(M a[], int len)
{
    int i,j,t;
    for(i=0; i<len-1; i++)
    {
        for(j=0; j<len-1-i; j++)
        {
            if(a[j].num>a[j+1].num)
            {
                t = a[j].num;
                a[j].num = a[j+1].num;
                a[j+1].num = t;
            }
        }
    }
}

void Merge(M elmnts[], M relmnts[], int n, int flag)
{
    int mi, ti, ri;
    M temp [200];
    
    mi = ri = ti = 0;
    while (mi < n && ri < n)
    {
        if (elmnts[mi].num >= relmnts[ri].num)
        {
            temp[ti].num = relmnts[ri].num;
            ri++;
            ti++;
        }
        else
        {
            temp[ti].num = elmnts[mi].num;
            ti++;
            mi++;
        }
    }
    while (mi < n)
    {
        temp[ti].num = elmnts[mi].num;
        ti++;
        mi++;
    }
    while (ri < n)
    {
        temp[ti].num = relmnts[ri].num;
        ti++;
        ri++;
    }
    ti = flag > 0 ? n : 0;

    for (mi = 0; mi < n; mi++)
        elmnts[mi].num = temp[ti++].num;  
}

int getNeighbor(int round, int myrank, int npes)
{
    int neighbor;
    if (round % 2 == 0)//even round
    {
        if (myrank % 2 == 0)// neighbor is left
            neighbor = myrank - 1;
        else                 
            neighbor = myrank + 1;
    }
    else
    {
        if (myrank % 2 == 0)
            neighbor = myrank + 1;
        else
            neighbor = myrank - 1;
    }
    if (neighbor == -1 || neighbor == npes)
        neighbor = MPI_PROC_NULL;
    return neighbor;
}

void Print(M h[])
{
    int i = 0;
    while( i < n )
    {
        if(h[i].count != 0)
        {
            cout << "Number: " << h[i].num << "   Counts: " << h[i].count << endl;
        } 
        i++; 
    } 
       
}
  

