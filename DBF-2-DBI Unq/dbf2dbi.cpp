/* ******************************************************************** **
** @@ DBX DBF-2-DBI indexer
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Note   :
** ******************************************************************** */

/* ******************************************************************** **
**                uses pre-compiled headers
** ******************************************************************** */

#include "stdafx.h"

#include <stdio.h>
#include <limits.h>
#include <time.h>

#include "..\shared\xlat_tables.h"
#include "..\shared\xlat.h"
#include "..\shared\text.h"
#include "..\shared\file.h"
#include "..\shared\file_walker.h"
#include "..\shared\mmf.h"
#include "..\shared\timestamp.h"
#include "..\shared\vector.h"
#include "..\shared\vector_sorted.h"
#include "..\shared\db_dbx.h"
#include "..\shared\search_bmh.h"
#include "..\shared\hash_murmur3.h"
#include "..\shared\map_bppt_jannink.h"

#ifdef NDEBUG
#pragma optimize("gsy",on)
#pragma comment(linker,"/FILEALIGN:512 /MERGE:.rdata=.text /MERGE:.data=.text /SECTION:.text,EWR /IGNORE:4078")
#endif

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/* ******************************************************************** **
** @@                   internal defines
** ******************************************************************** */

#ifndef QWORD
typedef unsigned __int64   QWORD;
#endif

#define  MAX_RECORD_SIZE                        (128)

/* ******************************************************************** **
** @@                   internal prototypes
** ******************************************************************** */

/* ******************************************************************** **
** @@                   external global variables
** ******************************************************************** */

extern DWORD   dwKeepError = 0;

/* ******************************************************************** **
** @@                   static global variables
** ******************************************************************** */

static DWORD         _dwSizeOrg = 0;
static DWORD         _pHashOrg[4];

static DWORD         _dwGranulation = 3; // 2 Power: 0, 2, 3, 4

static char          _pRecord[MAX_RECORD_SIZE];

static DBX_TABLE_INFO      _InfoSrc;
static DBX_TABLE_INFO      _InfoDst;
                              
static BPPTreeIndex        _Index;

// static FILE*         _pOut = NULL;

/* ******************************************************************** **
** @@                   real code
** ******************************************************************** */

/* ******************************************************************** **
** @@ CMP_Hash()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  : Hash128 compare function
** ******************************************************************** */

inline int CMP_Hash(const void* const pKey1,const void* const pKey2)
{
   DWORD*  p1 = (DWORD*)pKey1;
   DWORD*  p2 = (DWORD*)pKey2;

   if (p1[0] > p2[0])
   {
      return 1;
   }
   else if (p1[0] < p2[0])
   {
      return -1;
   }
   else
   {
      if (p1[1] > p2[1])
      {
         return 1;
      }
      else if (p1[1] < p2[1])
      {
         return -1;
      }
      else
      {
         if (p1[2] > p2[2])
         {
            return 1;
         }
         else if (p1[2] < p2[2])
         {
            return -1;
         }
         else
         {
            if (p1[3] > p2[3])
            {
               return 1;
            }
            else if (p1[3] < p2[3])
            {
               return -1;
            }
            else
            {
               return 0;
            }
         }
      }
   }
}

/* ******************************************************************** **
** @@ Insert()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void Insert
(
   const DWORD* const      pHash,
   DWORD                   dwIndex
)
{
   ASSERT(pHash);
   ASSERT(dwIndex);

/*
   char     pszSignature[MAX_PATH];

   for (int kk = 0; kk < 32; ++kk)
   {
      sprintf(&pszSignature[kk * 2],"%02X",((BYTE*)pHash)[kk]);
   }

   pszSignature[32] = 0; // Ensure ASCIIZ

//   fprintf(_pOut,"%08X  %s\n",dwIndex,pszSignature);

   if (!_Index.Insert((char*)pHash,dwIndex))
   {
      fprintf(_pOut,"*** Collision at Idx: [%08X] : %08X : %s\n",dwIndex,dwSizeOrg,pszSignature);
   }
*/
   _Index.Insert((char*)pHash,dwIndex);
}

/* ******************************************************************** **
** @@ Index()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void Index
(
   const char* const       pszFile,
   const char* const       pszIdx
)
{
   ASSERT(pszFile);
   ASSERT(pszIdx);

   char     pszDBFName [_MAX_PATH];
   char     pszDrive   [_MAX_DRIVE];
   char     pszDir     [_MAX_DIR];
   char     pszFName   [_MAX_FNAME];
   char     pszExt     [_MAX_EXT];

   _splitpath(pszFile,    pszDrive,pszDir,pszFName,pszExt);
   _makepath( pszDBFName, pszDrive,pszDir,pszFName,"dbf");

   DBX*           pDBF = new DBX;

   DBX_TABLE*     pSrc = pDBF->OpenTable(pszFName,pszDBFName,NULL,DBX_OM_READ_ONLY,DBX_OM_READ_ONLY);
   
   if (!pSrc)
   {
      // Error !
      ASSERT(0);
      return;
   }

   DWORD    dwRecCnt = pSrc->GetRecCnt();

   ASSERT(dwRecCnt);

   for (DWORD ii = 1; ii <= dwRecCnt; ++ii)
   {
      const BYTE* const    pRecord = pSrc->Go(ii);

      ASSERT(pRecord);

      DBX_COLUMN*    pIndex   = pSrc->GetColumn("INDEX");
      DBX_COLUMN*    pGuid    = pSrc->GetColumn("GUID");
      DBX_COLUMN*    pPos     = pSrc->GetColumn("TEXT");

      ASSERT(pIndex);
      ASSERT(pGuid);
      ASSERT(pPos);

      DWORD    dwIndex   = *(DWORD*)pIndex->  Get(pRecord);

      DWORD    pHash[4]; // 128 bits Murmur hash
      
      memcpy(pHash,pGuid->Get(pRecord),sizeof(DWORD) * 4);
      
//      DWORD    dwPos = *(DWORD*)pPos->Get(pRecord);

      Insert(pHash,dwIndex);
   }

   pDBF->CloseTable(pSrc);

   delete pDBF;
   pDBF = NULL;
}

/* ******************************************************************** **
** @@ ForEach()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void ForEach(const char* const pszFilename)
{
   char     pszIdxName[_MAX_PATH];
   char     pszDrive  [_MAX_DRIVE];
   char     pszDir    [_MAX_DIR];
   char     pszFName  [_MAX_FNAME];
   char     pszExt    [_MAX_EXT];

   _splitpath(pszFilename,pszDrive,pszDir,pszFName,pszExt);
   _makepath( pszIdxName, pszDrive,pszDir,pszFName,"dbi");

   BPPT_INDEX_INFO      Info;

   memset(&Info,0,sizeof(Info));

   strcpy(Info._pszIndexName,pszIdxName);

   Info._pCompare = CMP_Hash;

   Info._bDuplicate   = false;
   Info._bInitialized = false;  
                                    
   Info._iKeySize    = 16;          // 128 bits Murmur Hash 
   Info._iSectorSize = (1 << 16);   // 64 K

   if (!_Index.Open(Info))
   {
      // Error !
      ASSERT(0);
      _Index.Close();
      return;
   }

   Index(pszFilename,pszIdxName);

   _Index.Close();
}

/* ******************************************************************** **
** @@ ShowHelp()
** @  Copyrt :
** @  Author :
** @  Modify :
** @  Update :
** @  Notes  :
** ******************************************************************** */

static void ShowHelp()
{
   const char  pszCopyright[] = "-*-   DBF-2-DBI  1.0   *   Copyright (c) Gazlan, 2015   -*-";
   const char  pszDescript [] = "DBX DBF-2-DBI indexer";
   const char  pszE_Mail   [] = "complains_n_suggestions direct to gazlan@yandex.ru";

   printf("%s\n\n",pszCopyright);
   printf("%s\n\n",pszDescript);
   printf("Usage: dbf2dbi.com wildcards\n\n");
   printf("%s\n",pszE_Mail);
}

/* ******************************************************************** **
** @@ main()
** @ Copyrt:
** @ Author:
** @ Modify:
** @ Update:
** @ Notes :
** ******************************************************************** */

int main(int argc,char** argv)
{
   if (argc != 2)
   {
      ShowHelp();
      return 0;
   }

   if (argc == 2 && ((!strcmp(argv[1],"?")) || (!strcmp(argv[1],"/?")) || (!strcmp(argv[1],"-?")) || (!stricmp(argv[1],"/h")) || (!stricmp(argv[1],"-h"))))
   {
      ShowHelp();
      return 0;
   }

//   _pOut = fopen("err_log.txt","wt");

   char     pszMask[MAX_PATH + 1];
   
   memset(pszMask,0,sizeof(pszMask));
   
   strncpy(pszMask,argv[1],MAX_PATH);
   pszMask[MAX_PATH] = 0; // Ensure ASCIIZ
   
   char     pszDrive[_MAX_DRIVE];
   char     pszDir  [_MAX_DIR];
   char     pszFName[_MAX_FNAME];
   char     pszExt  [_MAX_EXT];
   
   _splitpath(pszMask,pszDrive,pszDir,pszFName,pszExt);
   
   char     pszSrchMask[MAX_PATH + 1];
   char     pszSrchPath[MAX_PATH + 1];
   
   strcpy(pszSrchMask,pszFName);
   strcat(pszSrchMask,pszExt);
   
   Walker      Visitor;

   Visitor.Init(ForEach,pszSrchMask,false);

   strcpy(pszSrchPath,pszDrive);
   strcat(pszSrchPath,pszDir);

   Visitor.Run(*pszSrchPath  ?  pszSrchPath  :  ".");

/*
   if (_pOut)
   {
      fclose(_pOut);
      _pOut = NULL;
   }
*/

   return 0;
}

/* ******************************************************************** **
**                That's All
** ******************************************************************** */
