/* ******************************************************************** **
** @@ Walker
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update : 
** @  Dscr   : Directory Walker
** ******************************************************************** */

#ifndef _FILE_WALKER_HPP_
#define _FILE_WALKER_HPP_
 
#if _MSC_VER > 1000
#pragma once
#endif

/* ******************************************************************** **
** @@                   internal defines
** ******************************************************************** */

typedef  void (* USER_FUNC)(const char* const);

/* ******************************************************************** **
** @@                   internal prototypes
** ******************************************************************** */

/* ******************************************************************** **
** @@                   external global variables
** ******************************************************************** */

/* ******************************************************************** **
** @@                   static global variables
** ******************************************************************** */

/* ******************************************************************** **
** @@                   Classes
** ******************************************************************** */

/* ******************************************************************** **
** @@ class Walker
** @  Copyrt : 
** @  Author : 
** @  Modify :
** @  Update : 
** @  Dscr   :
** ******************************************************************** */

class Walker
{
   private:

      char              _pszMask[MAX_PATH + 1];
      bool              _bRecursive;
      USER_FUNC         _pHandler;

   public:
   
       Walker();
      ~Walker();

      void  Init(USER_FUNC pHandler,const char* const pszMask = NULL,bool bRecursive = false); 
      bool  Run(const char* const pszRoot);

   private:

      void  Reset();
};

#endif

/* ******************************************************************** **
** @@                   The End
** ******************************************************************** */
