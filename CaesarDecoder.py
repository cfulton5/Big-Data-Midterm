from pyspark import *
import string
import sys
import collections

if __name__ == "__main__":

    sc = SparkContext("local","Ceasar Decoder")


    def readDictionary():       #Open and assign all 479k words in the English language to a string from a text file I foundWords on GitHub (lol)
        with open("D:/spark/wordDictionary.txt") as word_file:
            valid_words = set(word_file.read().split())
        return valid_words
    
    def freqLetterFinder(index):                                  #find most frequent letter in the text file                    
        characterMap = characters.map(lambda character: (character.lower(), 1)).reduceByKey(lambda a, b: a + b).collect()
        characterMap.sort(key=lambda character2: character2[1], reverse = True)
        character = characterMap[index][0]              
        if str(character) not in alphabet:     
            return freqLetterFinder(index + 1)
        else:
            return character
   
    def shiftKey(mostCommonLetter, frequentLetters):    #finds the difference between the most common letter in the text and the statistically most common letter
        freqASCIIIndex = string.ascii_lowercase.index(frequentLetters.lower())      ##sorry whoever's grading this; it's hard to keep track of "frequent" vs "common" throughout this project, but bear with me- I got it to work
        commonASCIIIndex = string.ascii_lowercase.index(mostCommonLetter.lower())
        return freqASCIIIndex-commonASCIIIndex
        
    def shifter(encryptedText, key):    #shifts all the letters in the text file by adding shifted characters to empty string
        shiftedText = ""
        for character in encryptedText:
            if character.lower() in alphabet:
                shiftIndex = string.ascii_lowercase.index(character.lower()) + key
                if shiftIndex - 1 < 26:
                    shiftIndex = shiftIndex + 26
                if shiftIndex - 1 > 1:  
                    shiftIndex = shiftIndex - 26
                shiftedText = shiftedText + alphabet[shiftIndex]
            else:
                shiftedText = shiftedText + character
        return shiftedText
    
    def dictionaryLookUp(text):         #searches the dictionary string for each word in the decrypted text to check for correctness
        foundWords = 0
        lengthOfDecryptedText = len(text)
        for _ in range(lengthOfDecryptedText):
            index = 0
            word = ''.join(c for c in text[index] if str(c).isalnum()).lower()
            if word in dictionary:  
                foundWords = foundWords + 1
            if not word:
                lengthOfDecryptedText = lengthOfDecryptedText - 1
        if (foundWords / lengthOfDecryptedText) * 100 > 50:
            return True
        else:    
            return False
        
    def ceasarDecryption(statisticallyFrequentLetters):         #all together, now          ...and printing intermediate steps for question #2
        Decrypting = True
        statisticallyCommonLetterIndex = 0
        mostCommonLetter = freqLetterFinder(0)
        while Decrypting:
            statisticallyFrequentLettersIndex = statisticallyFrequentLetters[statisticallyCommonLetterIndex]
            key = shiftKey(mostCommonLetter, statisticallyFrequentLettersIndex)
            joinItUp = characters.collect()
            shifted = shifter(joinItUp, key)
            shiftedSplitUp = shifted.split()
            if dictionaryLookUp(shiftedSplitUp):
                print(joinItUp)
                print(key)
                print(shiftedSplitUp)
                Decrypting = False
            else:
                statisticallyCommonLetterIndex = statisticallyCommonLetterIndex + 1
        return  shifted
            
    alphabet = 'abcdefghijklmnopqrstuvwxyz'         #English Alphabet not including capitals because that messes just about everything up
    statisticallyFrequentLetters = ['e', 't', 'a', 'o', 'i']    #the most common letters used in the alphabet 
    dictionary = readDictionary()                               #all the words in the English Language
    encryptedTextFile = sc.textFile("D:/spark/Encrypted-2.txt").cache()      #the text file to decrypt
    characters = encryptedTextFile.flatMap(lambda line: list(line))          #flatmap of the characteracters in the text file
   
    decryptedText = ceasarDecryption(statisticallyFrequentLetters)      #the decryption method call
        
    with open("D:/spark/Decrypted-2.txt", "w") as text_file:      #save decrypted text as a text file
        print(f"{decryptedText}", file=text_file)